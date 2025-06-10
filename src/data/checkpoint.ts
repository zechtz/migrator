import { promises as fs } from "fs";
import { logInfo, logWarn, logError } from "../utils/logger.js";

export interface CheckpointData {
  lastProcessedId: number;
  totalProcessed: number;
  timestamp: string;
  tableProgress?: Record<
    string,
    {
      lastProcessedId: number;
      totalProcessed: number;
      isComplete: boolean;
      lastCursorValue?: any;
    }
  >;
}

/**
 * Fixed checkpoint saving that handles directory path issues
 */
export const saveCheckpoint = async (
  checkpointFile: string,
  lastProcessedId: number,
  totalProcessed: number,
): Promise<void> => {
  try {
    // Ensure we have a filename, not a directory
    let filePath = checkpointFile;
    if (checkpointFile.endsWith("/") || !checkpointFile.includes(".")) {
      filePath =
        checkpointFile.replace(/\/$/, "") + "/migration_checkpoint.json";
    }

    const checkpointData: CheckpointData = {
      lastProcessedId,
      totalProcessed,
      timestamp: new Date().toISOString(),
    };

    await fs.writeFile(filePath, JSON.stringify(checkpointData, null, 2));
    await logInfo(`✅ Checkpoint saved: ${totalProcessed} rows processed`);
  } catch (error) {
    await logWarn(`⚠️ Could not save checkpoint: ${error}`);
    // Don't throw - let migration continue
  }
};

/**
 * Load checkpoint with better error handling
 */
export const loadCheckpoint = async (
  checkpointFile: string,
): Promise<CheckpointData> => {
  try {
    // Ensure we have a filename, not a directory
    let filePath = checkpointFile;
    if (checkpointFile.endsWith("/") || !checkpointFile.includes(".")) {
      filePath =
        checkpointFile.replace(/\/$/, "") + "/migration_checkpoint.json";
    }

    const data = await fs.readFile(filePath, "utf8");
    const checkpoint = JSON.parse(data);
    await logInfo(
      `📋 Loaded checkpoint: ${checkpoint.totalProcessed} rows previously processed`,
    );
    return checkpoint;
  } catch (error) {
    await logWarn(`⚠️ Could not load checkpoint: ${error}`);
    return {
      lastProcessedId: 0,
      totalProcessed: 0,
      timestamp: new Date().toISOString(),
    };
  }
};
