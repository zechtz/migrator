import { MigrationConfig, MigrationTask } from "types";
import {
  initializeConnections,
  closeConnections,
} from "../database/connections.js";
import { loadCheckpoint } from "../data/checkpoint.js";
import { ConcurrentMigrator } from "../migration/concurrent-migrator.js";
import { logInfo, logError } from "../utils/logger.js";

export const runMigration = async (
  config: MigrationConfig,
  migrations: MigrationTask[],
): Promise<void> => {
  let connections = null;

  try {
    connections = await initializeConnections(config);
    await logInfo("Migration system initialized successfully");

    const checkpoint = await loadCheckpoint(config.checkpointFile);

    // Create concurrent migrator
    const migrator = new ConcurrentMigrator(connections, config, checkpoint);

    // Start progress monitoring
    const progressInterval = setInterval(async () => {
      const progress = await migrator.getProgress();
      await logInfo(
        `Progress: ${progress.completedTables}/${progress.totalTables} tables complete, ` +
          `${progress.totalRows} total rows processed, ` +
          `${progress.activeBatches} active batches, ` +
          `${progress.queuedBatches} queued batches`,
      );
    }, 30000); // Log progress every 30 seconds

    try {
      // Run concurrent migration
      await migrator.migrateTablesConcurrently(migrations);
      await logInfo("All migrations completed successfully");
    } finally {
      clearInterval(progressInterval);
    }
  } catch (error) {
    await logError(`Migration failed: ${(error as Error).message}`);
    throw error;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

// Legacy function for single-threaded migration (backward compatibility)
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
