import { MigrationConfig, MigrationTask } from "../types/index.js";
import {
  initializeConnections,
  closeConnections,
} from "../database/connections.js";
import { loadCheckpoint } from "../data/checkpoint.js";
import {
  migrateTablesConcurrently,
  getMigrationProgress,
} from "./concurrent-migrator.js";
import { logInfo, logError } from "../utils/logger.js";

/**
 * Run migration with foreign key resolution
 */
export const runMigration = async (
  config: MigrationConfig,
  migrations: MigrationTask[],
): Promise<void> => {
  let connections = null;

  try {
    connections = await initializeConnections(config);
    await logInfo("🚀 Migration system initialized successfully");

    const checkpoint = await loadCheckpoint(config.checkpointFile);

    // Start progress monitoring
    let progressInterval: NodeJS.Timeout | null = null;

    // Create a simple batch queue mock for progress monitoring
    const mockBatchQueue = { activeCount: 0, queueLength: 0 };

    progressInterval = setInterval(async () => {
      const progress = await getMigrationProgress(
        checkpoint,
        mockBatchQueue as any,
      );
      await logInfo(
        `📊 Progress: ${progress.completedTables}/${progress.totalTables} tables complete, ` +
          `${progress.totalRows} total rows processed, ` +
          `${progress.activeBatches} active batches, ` +
          `${progress.queuedBatches} queued batches`,
      );
    }, 30000); // Log progress every 30 seconds

    try {
      // Run enhanced concurrent migration
      await migrateTablesConcurrently(
        connections,
        config,
        checkpoint,
        migrations,
      );
      await logInfo("✅ All migrations completed successfully");
    } finally {
      if (progressInterval) {
        clearInterval(progressInterval);
      }
    }
  } catch (error) {
    await logError(`❌ Migration failed: ${(error as Error).message}`);
    throw error;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

/**
 * Legacy function for single-threaded migration (backward compatibility)
 */
export const runSequentialMigration = async (
  config: MigrationConfig,
  migrations: MigrationTask[],
): Promise<void> => {
  // Temporarily override concurrency settings for sequential processing
  const sequentialConfig = {
    ...config,
    maxConcurrentBatches: 1,
    maxConcurrentTables: 1,
  };

  await runMigration(sequentialConfig, migrations);
};
