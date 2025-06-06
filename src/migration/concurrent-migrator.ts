import {
  DatabaseConnections,
  MigrationConfig,
  Checkpoint,
  BatchJob,
  MigrationTask,
  PaginationContext,
  BatchResult,
} from "../types/index.js";
import {
  fetchOracleDataWithPagination,
  estimateTableRowCount,
  checkIndexExists,
} from "../database/oracle.js";
import { insertPostgresData } from "../database/postgres.js";
import { transformData } from "../data/transform.js";
import { saveCheckpoint } from "../data/checkpoint.js";
import { retryOperation } from "../utils/helpers.js";
import { Semaphore, AsyncQueue } from "../utils/semaphore.js";
import {
  determineBestPaginationStrategy,
  validatePaginationConfig,
} from "../database/pagination.js";
import { logInfo, logError, logWarn } from "../utils/logger.js";
import {
  buildMappingCache,
  createResolvers,
} from "../utils/foreign-key-resolver.js";
import { COMMON_RESOLVERS } from "../utils/foreign-key-resolvers.js";

// Type for cache configurations
interface CacheConfig {
  codeCol: string;
  idCol: string;
}

export class ConcurrentMigrator {
  private connections: DatabaseConnections;
  private config: MigrationConfig;
  private tableSemaphore: Semaphore;
  private batchSemaphore: Semaphore;
  private batchQueue: AsyncQueue<BatchJob, BatchResult>;
  private checkpoint: Checkpoint;

  constructor(
    connections: DatabaseConnections,
    config: MigrationConfig,
    checkpoint: Checkpoint,
  ) {
    this.connections = connections;
    this.config = config;
    this.checkpoint = checkpoint;

    this.tableSemaphore = new Semaphore(config.maxConcurrentTables);
    this.batchSemaphore = new Semaphore(config.maxConcurrentBatches);

    this.batchQueue = new AsyncQueue<BatchJob, BatchResult>(
      (job: BatchJob) => this.processBatch(job),
      config.maxConcurrentBatches,
    );
  }

  async migrateTablesConcurrently(migrations: MigrationTask[]): Promise<void> {
    await logInfo(
      `Starting concurrent migration of ${migrations.length} tables`,
    );

    // Sort migrations by priority (lower number = higher priority for hierarchical data)
    const sortedMigrations = migrations.sort(
      (a, b) => (a.priority || 0) - (b.priority || 0),
    );

    // Create promises for each table migration
    const migrationPromises = sortedMigrations.map((migration, index) =>
      this.migrateTableWithConcurrency(migration, `table_${index}`),
    );

    // Wait for all migrations to complete
    const results = await Promise.allSettled(migrationPromises);

    const failures = results.filter((result) => result.status === "rejected");
    if (failures.length > 0) {
      await logError(`${failures.length} table migrations failed`);
      throw new Error(`Migration failed for ${failures.length} tables`);
    }

    await logInfo("All table migrations completed successfully");
  }

  private async migrateTableWithConcurrency(
    migration: MigrationTask,
    tableId: string,
  ): Promise<number> {
    await this.tableSemaphore.acquire();

    try {
      const result = await this.migrateTableInternal(migration, tableId);

      // Build mapping cache after table completion (if it's a reference table)
      await this.onTableCompleted(migration.targetTable);

      return result;
    } finally {
      this.tableSemaphore.release();
    }
  }

  // Build mapping cache after reference tables complete
  private async onTableCompleted(targetTable: string): Promise<void> {
    const cacheConfigs: Record<string, CacheConfig> = {
      "crvs_global.tbl_delimitation_region": { codeCol: "code", idCol: "id" },
      "crvs_global.tbl_delimitation_district": { codeCol: "code", idCol: "id" },
      "crvs_global.tbl_delimitation_council": { codeCol: "code", idCol: "id" },
      "crvs_global.tbl_delimitation_ward": {
        codeCol: "code",
        idCol: "ward_id",
      },
      "crvs_global.tbl_mgt_health_facility": {
        codeCol: "code",
        idCol: "health_facility_id",
      },
    };

    const config = cacheConfigs[targetTable];
    if (config) {
      try {
        await buildMappingCache(
          this.connections.postgresPool,
          targetTable,
          config.codeCol,
          config.idCol,
        );
        await logInfo(`✅ Built mapping cache for ${targetTable}`);
      } catch (error) {
        await logWarn(`⚠️ Failed to build cache for ${targetTable}: ${error}`);
        // Continue migration - cache building is not critical for tables without dependencies
      }
    }
  }

  private async migrateTableInternal(
    migration: MigrationTask,
    tableId: string,
  ): Promise<number> {
    const { sourceQuery, targetTable } = migration;

    await logInfo(`Starting migration for table: ${targetTable}`);

    if (!this.checkpoint.tableProgress) {
      this.checkpoint.tableProgress = {};
    }

    let tableProgress = this.checkpoint.tableProgress[tableId] || {
      lastProcessedId: 0,
      totalProcessed: 0,
      isComplete: false,
    };

    if (tableProgress.isComplete) {
      await logInfo(`Table ${targetTable} already completed, skipping`);
      return tableProgress.totalProcessed;
    }

    const paginationStrategy = await this.determinePaginationStrategy(
      migration,
      sourceQuery,
    );

    validatePaginationConfig(
      paginationStrategy.strategy,
      paginationStrategy.cursorColumn,
      paginationStrategy.orderByClause,
    );

    let hasMore = true;
    let offset = tableProgress.lastProcessedId;
    let lastCursorValue = tableProgress.lastCursorValue;
    let batchNumber = 0;

    while (hasMore) {
      const maxConcurrentBatches =
        migration.maxConcurrentBatches || this.config.maxConcurrentBatches;

      const batchJobs: BatchJob[] = [];

      for (let i = 0; i < maxConcurrentBatches && hasMore; i++) {
        const batchJob: BatchJob = {
          tableId,
          batchNumber: batchNumber++,
          offset,
          batchSize: this.config.batchSize,
          sourceQuery,
          targetTable,
          transformFn: migration.transformFn, // Use the transform function from migration
          paginationStrategy: paginationStrategy.strategy,
          cursorColumn: paginationStrategy.cursorColumn,
          lastCursorValue,
        };

        batchJobs.push(batchJob);

        if (
          paginationStrategy.strategy === "offset" ||
          paginationStrategy.strategy === "rownum"
        ) {
          offset += this.config.batchSize;
        }
      }

      const batchResults = await this.processBatchesConcurrently(batchJobs);

      let totalProcessedInBatch = 0;
      for (const result of batchResults) {
        totalProcessedInBatch += result.processedCount;
        if (result.lastCursorValue !== undefined) {
          lastCursorValue = result.lastCursorValue;
        }
        hasMore = hasMore && !result.finished;
      }

      tableProgress.totalProcessed += totalProcessedInBatch;
      tableProgress.lastProcessedId = offset;
      tableProgress.lastCursorValue = lastCursorValue;

      this.checkpoint.tableProgress![tableId] = tableProgress;
      await saveCheckpoint(
        this.config.checkpointFile,
        tableProgress.lastProcessedId,
        tableProgress.totalProcessed,
      );

      await logInfo(
        `Table ${targetTable}: Processed ${totalProcessedInBatch} rows in batch. Total: ${tableProgress.totalProcessed}`,
      );

      if (totalProcessedInBatch === 0) {
        hasMore = false;
      }
    }

    tableProgress.isComplete = true;
    this.checkpoint.tableProgress![tableId] = tableProgress;

    await logInfo(
      `Completed migration for table ${targetTable}. Total rows: ${tableProgress.totalProcessed}`,
    );
    return tableProgress.totalProcessed;
  }

  private async processBatchesConcurrently(
    batchJobs: BatchJob[],
  ): Promise<BatchResult[]> {
    const promises = batchJobs.map((job) =>
      retryOperation(
        () => this.processBatch(job),
        this.config.maxRetries,
        this.config.retryDelay,
        `Batch ${job.batchNumber} for table ${job.targetTable}`,
      ),
    );

    return Promise.all(promises);
  }

  private async processBatch(job: BatchJob): Promise<BatchResult> {
    await this.batchSemaphore.acquire();

    try {
      const paginationContext: PaginationContext = {
        strategy: job.paginationStrategy,
        offset: job.offset,
        batchSize: job.batchSize,
        cursorColumn: job.cursorColumn,
        lastCursorValue: job.lastCursorValue,
        orderByClause:
          job.paginationStrategy === "cursor"
            ? `ORDER BY ${job.cursorColumn} ASC`
            : undefined,
      };

      await logInfo(
        `Processing batch ${job.batchNumber} for ${job.targetTable}, offset: ${job.offset}`,
      );

      const result = await fetchOracleDataWithPagination(
        this.connections.oracle,
        job.sourceQuery,
        paginationContext,
      );

      if (result.rows.length === 0) {
        return { finished: true, processedCount: 0 };
      }

      // Create resolvers for enhanced transform functions
      const resolvers = this.createResolversForTransform(job.targetTable);

      // Transform data with resolvers if it's an enhanced function
      const transformedData = await transformData(
        result.rows,
        job.transformFn,
        resolvers,
      );

      await insertPostgresData(
        this.connections.postgresPool,
        transformedData,
        job.targetTable,
      );

      return {
        finished: !result.hasMore,
        processedCount: transformedData.length,
        lastCursorValue: result.lastCursorValue,
      };
    } finally {
      this.batchSemaphore.release();
    }
  }

  // Create resolvers for enhanced transform functions
  private createResolversForTransform(
    targetTable: string,
  ): Record<string, any> | undefined {
    // Only create resolvers for tables that need foreign key resolution
    const tablesNeedingResolvers = [
      "crvs_global.tbl_delimitation_district",
      "crvs_global.tbl_delimitation_council",
      "crvs_global.tbl_delimitation_ward",
      "crvs_global.tbl_mgt_health_facility",
      "crvs_global.tbl_mgt_registration_center",
    ];

    if (!tablesNeedingResolvers.includes(targetTable)) {
      return undefined;
    }

    return createResolvers([
      COMMON_RESOLVERS.region,
      COMMON_RESOLVERS.district,
      COMMON_RESOLVERS.council,
      COMMON_RESOLVERS.ward,
      COMMON_RESOLVERS.healthFacility,
    ]);
  }

  private async determinePaginationStrategy(
    migration: MigrationTask,
    sourceQuery: string,
  ): Promise<{
    strategy: "rownum" | "offset" | "cursor";
    cursorColumn?: string;
    orderByClause?: string;
  }> {
    if (migration.paginationStrategy) {
      return {
        strategy: migration.paginationStrategy,
        cursorColumn: migration.cursorColumn,
        orderByClause: migration.orderByClause,
      };
    }

    if (this.config.paginationStrategy !== "rownum") {
      return {
        strategy: this.config.paginationStrategy,
        cursorColumn: this.config.cursorColumn,
        orderByClause: migration.orderByClause,
      };
    }

    try {
      const tableMatch = sourceQuery.match(/FROM\s+(\w+)/i);
      if (!tableMatch) {
        await logWarn(
          "Could not extract table name from query, using ROWNUM pagination",
        );
        return { strategy: "rownum" };
      }

      const tableName = tableMatch[1];
      const estimatedRows = await estimateTableRowCount(
        this.connections.oracle,
        tableName,
      );

      const hasIndex = migration.cursorColumn
        ? await checkIndexExists(
            this.connections.oracle,
            tableName,
            migration.cursorColumn,
          )
        : false;

      const strategy = determineBestPaginationStrategy(estimatedRows, hasIndex);

      await logInfo(
        `Auto-selected ${strategy} pagination for table ${tableName} (estimated ${estimatedRows} rows)`,
      );

      return {
        strategy,
        cursorColumn: migration.cursorColumn,
        orderByClause: migration.orderByClause,
      };
    } catch (error) {
      await logWarn(
        `Error determining pagination strategy: ${(error as Error).message}, defaulting to ROWNUM`,
      );
      return { strategy: "rownum" };
    }
  }

  async getProgress(): Promise<{
    totalTables: number;
    completedTables: number;
    totalRows: number;
    activeBatches: number;
    queuedBatches: number;
  }> {
    const tableProgress = this.checkpoint.tableProgress || {};
    const completedTables = Object.values(tableProgress).filter(
      (p) => p.isComplete,
    ).length;
    const totalRows = Object.values(tableProgress).reduce(
      (sum, p) => sum + p.totalProcessed,
      0,
    );

    return {
      totalTables: Object.keys(tableProgress).length,
      completedTables,
      totalRows,
      activeBatches: this.batchQueue.activeCount,
      queuedBatches: this.batchQueue.queueLength,
    };
  }
}
