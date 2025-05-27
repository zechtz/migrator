import {
  DatabaseConnections,
  MigrationConfig,
  Checkpoint,
  BatchJob,
  MigrationTask,
  PaginationContext,
  BatchResult,
} from "types";
import {
  fetchOracleDataWithPagination,
  estimateTableRowCount,
  checkIndexExists,
} from "database/oracle";
import { insertPostgresData } from "database/postgres";
import { transformData } from "data/transform";
import { saveCheckpoint } from "data/checkpoint";
import { retryOperation } from "utils/helpers";
import { Semaphore, AsyncQueue } from "utils/semaphore";
import {
  determineBestPaginationStrategy,
  validatePaginationConfig,
} from "database/pagination";
import { logInfo, logError, logWarn } from "utils/logger";

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

    // Initialize semaphores for concurrency control
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

    // Sort migrations by priority (higher priority first)
    const sortedMigrations = migrations.sort(
      (a, b) => (b.priority || 0) - (a.priority || 0),
    );

    // Create promises for each table migration
    const migrationPromises = sortedMigrations.map((migration, index) =>
      this.migrateTableWithConcurrency(migration, `table_${index}`),
    );

    // Wait for all migrations to complete
    const results = await Promise.allSettled(migrationPromises);

    // Check for failures
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
    // Acquire table-level semaphore
    await this.tableSemaphore.acquire();

    try {
      return await this.migrateTableInternal(migration, tableId);
    } finally {
      this.tableSemaphore.release();
    }
  }

  private async migrateTableInternal(
    migration: MigrationTask,
    tableId: string,
  ): Promise<number> {
    const { sourceQuery, targetTable, transformFn } = migration;

    await logInfo(`Starting migration for table: ${targetTable}`);

    // Initialize or load table progress
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

    // Determine optimal pagination strategy
    const paginationStrategy = await this.determinePaginationStrategy(
      migration,
      sourceQuery,
    );

    // Validate pagination configuration
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

      // Create batches for concurrent processing
      const batchJobs: BatchJob[] = [];

      for (let i = 0; i < maxConcurrentBatches && hasMore; i++) {
        const batchJob: BatchJob = {
          tableId,
          batchNumber: batchNumber++,
          offset,
          batchSize: this.config.batchSize,
          sourceQuery,
          targetTable,
          transformFn,
          paginationStrategy: paginationStrategy.strategy,
          cursorColumn: paginationStrategy.cursorColumn,
          lastCursorValue,
        };

        batchJobs.push(batchJob);

        // Update offset for next batch (only relevant for offset-based pagination)
        if (
          paginationStrategy.strategy === "offset" ||
          paginationStrategy.strategy === "rownum"
        ) {
          offset += this.config.batchSize;
        }
      }

      // Process batches concurrently
      const batchResults = await this.processBatchesConcurrently(batchJobs);

      // Update progress
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

      // Save progress checkpoint
      this.checkpoint.tableProgress![tableId] = tableProgress;
      await saveCheckpoint(
        this.config.checkpointFile,
        tableProgress.lastProcessedId,
        tableProgress.totalProcessed,
      );

      await logInfo(
        `Table ${targetTable}: Processed ${totalProcessedInBatch} rows in batch. Total: ${tableProgress.totalProcessed}`,
      );

      // Break if no more data or if any batch indicates completion
      if (totalProcessedInBatch === 0) {
        hasMore = false;
      }
    }

    // Mark table as complete
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
    // Acquire batch-level semaphore
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

      // Transform data
      const transformedData = await transformData(result.rows, job.transformFn);

      // Insert into PostgreSQL
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

  private async determinePaginationStrategy(
    migration: MigrationTask,
    sourceQuery: string,
  ): Promise<{
    strategy: "rownum" | "offset" | "cursor";
    cursorColumn?: string;
    orderByClause?: string;
  }> {
    // Use strategy from migration config if specified
    if (migration.paginationStrategy) {
      return {
        strategy: migration.paginationStrategy,
        cursorColumn: migration.cursorColumn,
        orderByClause: migration.orderByClause,
      };
    }

    // Use global config strategy if specified
    if (this.config.paginationStrategy !== "rownum") {
      return {
        strategy: this.config.paginationStrategy,
        cursorColumn: this.config.cursorColumn,
        orderByClause: migration.orderByClause,
      };
    }

    // Auto-determine best strategy
    try {
      // Extract table name from query (basic extraction)
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

      // Check if there's an index on the cursor column
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
