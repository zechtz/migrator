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
  buildInitialCaches,
  buildCacheAfterMigration,
  createResolversEnhanced,
  getTablesNeedingResolvers,
  DEFAULT_CACHE_CONFIGS,
} from "../utils/foreign-key-resolver.js";
import { COMMON_RESOLVERS } from "../utils/foreign-key-resolvers.js";

/**
 * Migration state to track concurrency and progress
 */
interface MigrationState {
  connections: DatabaseConnections;
  config: MigrationConfig;
  checkpoint: Checkpoint;
  tableSemaphore: Semaphore;
  batchSemaphore: Semaphore;
  batchQueue: AsyncQueue<BatchJob, BatchResult>;
}

/**
 * Initialize migration state with semaphores and queues
 */
const initializeMigrationState = (
  connections: DatabaseConnections,
  config: MigrationConfig,
  checkpoint: Checkpoint,
): MigrationState => {
  const tableSemaphore = new Semaphore(config.maxConcurrentTables);
  const batchSemaphore = new Semaphore(config.maxConcurrentBatches);

  // Create batch processor function that has access to state
  const batchProcessor = (job: BatchJob) =>
    processBatch(
      {
        connections,
        config,
        checkpoint,
        tableSemaphore,
        batchSemaphore,
      } as MigrationState,
      job,
    );

  const batchQueue = new AsyncQueue<BatchJob, BatchResult>(
    batchProcessor,
    config.maxConcurrentBatches,
  );

  return {
    connections,
    config,
    checkpoint,
    tableSemaphore,
    batchSemaphore,
    batchQueue,
  };
};

/**
 * Main migration function - enhanced with proper sequential dependency handling
 */
export const migrateTablesConcurrently = async (
  connections: DatabaseConnections,
  config: MigrationConfig,
  checkpoint: Checkpoint,
  migrations: MigrationTask[],
): Promise<void> => {
  await logInfo(
    `🚀 Starting enhanced concurrent migration of ${migrations.length} tables`,
  );

  // Initialize migration state
  const state = initializeMigrationState(connections, config, checkpoint);

  // Sort migrations by priority (lower number = higher priority for hierarchical data)
  const sortedMigrations = migrations.sort(
    (a, b) => (a.priority || 0) - (b.priority || 0),
  );

  // Step 1: Build initial caches for reference tables that already exist
  await buildInitialCaches(connections.postgresPool, DEFAULT_CACHE_CONFIGS);

  // Step 2: Process migrations SEQUENTIALLY by priority groups to respect dependencies
  await processMigrationsByPriorityGroups(state, sortedMigrations);

  await logInfo("✅ All table migrations completed successfully");
};

/**
 * Process migrations in priority groups to ensure dependencies are met
 */
const processMigrationsByPriorityGroups = async (
  state: MigrationState,
  migrations: MigrationTask[],
): Promise<void> => {
  // Group migrations by priority
  const priorityGroups = new Map<number, MigrationTask[]>();

  migrations.forEach((migration) => {
    const priority = migration.priority || 0;
    if (!priorityGroups.has(priority)) {
      priorityGroups.set(priority, []);
    }
    priorityGroups.get(priority)!.push(migration);
  });

  // Process each priority group sequentially
  const sortedPriorities = Array.from(priorityGroups.keys()).sort(
    (a, b) => a - b,
  );

  for (const priority of sortedPriorities) {
    const groupMigrations = priorityGroups.get(priority)!;

    await logInfo(
      `📋 Processing priority group ${priority} (${groupMigrations.length} tables)`,
    );

    // Process all tables in this priority group concurrently
    const migrationPromises = groupMigrations.map((migration, index) =>
      migrateTableWithDependencies(
        state,
        migration,
        `table_p${priority}_${index}`,
      ),
    );

    const results = await Promise.allSettled(migrationPromises);

    const failures = results.filter((result) => result.status === "rejected");
    if (failures.length > 0) {
      await logError(
        `❌ ${failures.length} table migrations failed in priority group ${priority}`,
      );
      throw new Error(
        `Migration failed for ${failures.length} tables in priority group ${priority}`,
      );
    }

    await logInfo(`✅ Completed priority group ${priority}`);
  }
};

/**
 * Migrate single table with dependency management
 */
const migrateTableWithDependencies = async (
  state: MigrationState,
  migration: MigrationTask,
  tableId: string,
): Promise<number> => {
  await state.tableSemaphore.acquire();

  try {
    const result = await migrateTableInternal(state, migration, tableId);

    // Build cache after successful migration if this is a reference table
    await buildCacheAfterMigration(
      state.connections.postgresPool,
      migration.targetTable,
      DEFAULT_CACHE_CONFIGS,
    );

    return result;
  } finally {
    state.tableSemaphore.release();
  }
};

/**
 * Internal table migration logic
 */
const migrateTableInternal = async (
  state: MigrationState,
  migration: MigrationTask,
  tableId: string,
): Promise<number> => {
  const { sourceQuery, targetTable } = migration;

  await logInfo(`📋 Starting migration for table: ${targetTable}`);

  if (!state.checkpoint.tableProgress) {
    state.checkpoint.tableProgress = {};
  }

  let tableProgress = state.checkpoint.tableProgress[tableId] || {
    lastProcessedId: 0,
    totalProcessed: 0,
    isComplete: false,
  };

  if (tableProgress.isComplete) {
    await logInfo(`⏭️  Table ${targetTable} already completed, skipping`);
    return tableProgress.totalProcessed;
  }

  const paginationStrategy = await determinePaginationStrategyForMigration(
    state,
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
      migration.maxConcurrentBatches || state.config.maxConcurrentBatches;

    const batchJobs: BatchJob[] = [];

    for (let i = 0; i < maxConcurrentBatches && hasMore; i++) {
      const batchJob: BatchJob = {
        tableId,
        batchNumber: batchNumber++,
        offset,
        batchSize: state.config.batchSize,
        sourceQuery,
        targetTable,
        transformFn: migration.transformFn,
        paginationStrategy: paginationStrategy.strategy,
        cursorColumn: paginationStrategy.cursorColumn,
        lastCursorValue,
      };

      batchJobs.push(batchJob);

      if (
        paginationStrategy.strategy === "offset" ||
        paginationStrategy.strategy === "rownum"
      ) {
        offset += state.config.batchSize;
      }
    }

    const batchResults = await processBatchesConcurrently(state, batchJobs);

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

    state.checkpoint.tableProgress![tableId] = tableProgress;

    // Fix checkpoint file path issue
    try {
      await saveCheckpoint(
        state.config.checkpointFile,
        tableProgress.lastProcessedId,
        tableProgress.totalProcessed,
      );
    } catch (error) {
      await logWarn(`⚠️ Could not save checkpoint: ${error}`);
    }

    await logInfo(
      `📊 Table ${targetTable}: Processed ${totalProcessedInBatch} rows in batch. Total: ${tableProgress.totalProcessed}`,
    );

    if (totalProcessedInBatch === 0) {
      hasMore = false;
    }
  }

  tableProgress.isComplete = true;
  state.checkpoint.tableProgress![tableId] = tableProgress;

  await logInfo(
    `✅ Completed migration for table ${targetTable}. Total rows: ${tableProgress.totalProcessed}`,
  );
  return tableProgress.totalProcessed;
};

/**
 * Process multiple batches concurrently
 */
const processBatchesConcurrently = async (
  state: MigrationState,
  batchJobs: BatchJob[],
): Promise<BatchResult[]> => {
  const promises = batchJobs.map((job) =>
    retryOperation(
      () => processBatch(state, job),
      state.config.maxRetries,
      state.config.retryDelay,
      `Batch ${job.batchNumber} for table ${job.targetTable}`,
    ),
  );

  return Promise.all(promises);
};

/**
 * Process a single batch with enhanced foreign key resolution
 */
const processBatch = async (
  state: MigrationState,
  job: BatchJob,
): Promise<BatchResult> => {
  await state.batchSemaphore.acquire();

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
      `⚙️  Processing batch ${job.batchNumber} for ${job.targetTable}, offset: ${job.offset}`,
    );

    const result = await fetchOracleDataWithPagination(
      state.connections.oracle,
      job.sourceQuery,
      paginationContext,
    );

    if (result.rows.length === 0) {
      return { finished: true, processedCount: 0 };
    }

    // Create resolvers for enhanced transform functions
    const resolvers = createResolversForTransform(job.targetTable);

    // Debug: Log sample row to understand field mapping
    if (result.rows.length > 0 && job.targetTable.includes("district")) {
      await logInfo(
        `🔍 Sample ${job.targetTable} row fields: ${Object.keys(result.rows[0]).join(", ")}`,
      );

      // Check for region-related fields
      const sampleRow = result.rows[0];
      const regionFields = Object.keys(sampleRow).filter((key) =>
        key.toLowerCase().includes("region"),
      );

      if (regionFields.length > 0) {
        await logInfo(
          `🔍 Region-related fields found: ${regionFields.join(", ")}`,
        );
        regionFields.forEach((field) => {
          logInfo(`   ${field}: '${sampleRow[field]}'`);
        });
      } else {
        await logWarn(
          `⚠️ No region-related fields found in ${job.targetTable}`,
        );
      }
    }

    // Transform data with resolvers if it's an enhanced function
    const transformedData = await transformData(
      result.rows,
      job.transformFn,
      resolvers,
    );

    await insertPostgresData(
      state.connections.postgresPool,
      transformedData,
      job.targetTable,
    );

    return {
      finished: !result.hasMore,
      processedCount: transformedData.length,
      lastCursorValue: result.lastCursorValue,
    };
  } finally {
    state.batchSemaphore.release();
  }
};

/**
 * Create resolvers for enhanced transform functions with better debugging
 */
const createResolversForTransform = (
  targetTable: string,
): Record<string, any> | undefined => {
  const tablesNeedingResolvers = getTablesNeedingResolvers();

  if (!tablesNeedingResolvers.includes(targetTable)) {
    return undefined;
  }

  const resolvers = createResolversEnhanced([
    COMMON_RESOLVERS.region,
    COMMON_RESOLVERS.district,
    COMMON_RESOLVERS.council,
    COMMON_RESOLVERS.ward,
    COMMON_RESOLVERS.healthFacility,
  ]);

  // Log available resolvers for debugging
  logInfo(
    `🔧 Created resolvers for ${targetTable}: ${Object.keys(resolvers).join(", ")}`,
  );

  return resolvers;
};

/**
 * Determine pagination strategy for a migration
 */
const determinePaginationStrategyForMigration = async (
  state: MigrationState,
  migration: MigrationTask,
  sourceQuery: string,
): Promise<{
  strategy: "rownum" | "offset" | "cursor";
  cursorColumn?: string;
  orderByClause?: string;
}> => {
  if (migration.paginationStrategy) {
    return {
      strategy: migration.paginationStrategy,
      cursorColumn: migration.cursorColumn,
      orderByClause: migration.orderByClause,
    };
  }

  if (state.config.paginationStrategy !== "rownum") {
    return {
      strategy: state.config.paginationStrategy,
      cursorColumn: state.config.cursorColumn,
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
      state.connections.oracle,
      tableName,
    );

    const hasIndex = migration.cursorColumn
      ? await checkIndexExists(
          state.connections.oracle,
          tableName,
          migration.cursorColumn,
        )
      : false;

    const strategy = determineBestPaginationStrategy(estimatedRows, hasIndex);

    await logInfo(
      `🎯 Auto-selected ${strategy} pagination for table ${tableName} (estimated ${estimatedRows} rows)`,
    );

    return {
      strategy,
      cursorColumn: migration.cursorColumn,
      orderByClause: migration.orderByClause,
    };
  } catch (error) {
    await logWarn(
      `⚠️  Error determining pagination strategy: ${(error as Error).message}, defaulting to ROWNUM`,
    );
    return { strategy: "rownum" };
  }
};

/**
 * Get migration progress
 */
export const getMigrationProgress = async (
  checkpoint: Checkpoint,
  batchQueue: AsyncQueue<BatchJob, BatchResult>,
): Promise<{
  totalTables: number;
  completedTables: number;
  totalRows: number;
  activeBatches: number;
  queuedBatches: number;
}> => {
  const tableProgress = checkpoint.tableProgress || {};
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
    activeBatches: batchQueue.activeCount,
    queuedBatches: batchQueue.queueLength,
  };
};

/**
 * Legacy function for backward compatibility (updated to use functional approach)
 */
export const migrateTable = async (
  connections: DatabaseConnections,
  config: MigrationConfig,
  sourceQuery: string,
  targetTable: string,
  initialCheckpoint: Checkpoint,
  transformFn?: any,
): Promise<number> => {
  const migration: MigrationTask = {
    sourceQuery,
    targetTable,
    transformFn,
    priority: 1,
  };

  const state = initializeMigrationState(
    connections,
    config,
    initialCheckpoint,
  );
  return await migrateTableWithDependencies(
    state,
    migration,
    "legacy_migration",
  );
};
