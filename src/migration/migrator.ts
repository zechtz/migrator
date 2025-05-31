import {
  DatabaseConnections,
  MigrationConfig,
  Checkpoint,
  BatchResult,
  TransformFunction,
} from "../types/index.js";
import { fetchOracleData } from "../database/oracle.js";
import { insertPostgresData } from "../database/postgres.js";
import { transformData } from "../data/transform.js";
import { saveCheckpoint } from "../data/checkpoint.js";
import { retryOperation, sleep } from "../utils/helpers.js";
import { logInfo } from "../utils/logger.js";

export const migrateTable = async (
  connections: DatabaseConnections,
  config: MigrationConfig,
  sourceQuery: string,
  targetTable: string,
  initialCheckpoint: Checkpoint,
  transformFn?: TransformFunction,
): Promise<number> => {
  await logInfo(`Starting migration to ${targetTable}`);

  let offset = initialCheckpoint.lastProcessedId;
  let totalProcessed = initialCheckpoint.totalProcessed;
  let batchCount = 0;

  while (true) {
    const batchOperation = async (): Promise<BatchResult> => {
      await logInfo(`Fetching batch ${batchCount + 1}, offset: ${offset}`);

      const oracleData = await fetchOracleData(
        connections.oracle,
        sourceQuery,
        offset,
        config.batchSize,
      );

      if (oracleData.length === 0) {
        await logInfo("No more data to process");
        return { finished: true, processedCount: 0 };
      }

      const transformedData = await transformData(oracleData, transformFn);
      await insertPostgresData(
        connections.postgresPool,
        transformedData,
        targetTable,
      );

      return { finished: false, processedCount: transformedData.length };
    };

    const result = await retryOperation(
      batchOperation,
      config.maxRetries,
      config.retryDelay,
      `Batch ${batchCount + 1} processing`,
    );

    if (result.finished) {
      return totalProcessed;
    }

    totalProcessed += result.processedCount;
    offset += result.processedCount;
    batchCount++;

    await saveCheckpoint(config.checkpointFile, offset, totalProcessed);
    await logInfo(
      `Batch ${batchCount} completed. Total processed: ${totalProcessed}`,
    );
    await sleep(100);
  }
};
