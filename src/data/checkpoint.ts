import { promises as fs } from "fs";
import { dirname, resolve, extname } from "path";
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
      lastProcessedRecord?: any; // Store last record for deduplication
    }
  >;
}

/**
 * Fixed checkpoint saving that properly handles file paths
 */
export const saveCheckpoint = async (
  checkpointFile: string,
  lastProcessedId: number,
  totalProcessed: number,
  tableId?: string,
  lastProcessedRecord?: any,
): Promise<void> => {
  try {
    // Fix the file path - ensure it's a .json file
    let filePath = normalizeCheckpointPath(checkpointFile);

    // Ensure directory exists
    const dir = dirname(filePath);
    await fs.mkdir(dir, { recursive: true });

    // Load existing checkpoint or create new one
    let checkpointData: CheckpointData;
    try {
      const existingData = await fs.readFile(filePath, "utf8");
      checkpointData = JSON.parse(existingData);
    } catch {
      // File doesn't exist or is invalid, create new
      checkpointData = {
        lastProcessedId: 0,
        totalProcessed: 0,
        timestamp: new Date().toISOString(),
        tableProgress: {},
      };
    }

    // Update checkpoint data
    if (tableId) {
      // Update specific table progress
      if (!checkpointData.tableProgress) {
        checkpointData.tableProgress = {};
      }

      checkpointData.tableProgress[tableId] = {
        lastProcessedId,
        totalProcessed,
        isComplete: false,
        lastProcessedRecord, // Store for deduplication
      };
    } else {
      // Update global progress
      checkpointData.lastProcessedId = lastProcessedId;
      checkpointData.totalProcessed = totalProcessed;
    }

    checkpointData.timestamp = new Date().toISOString();

    await fs.writeFile(filePath, JSON.stringify(checkpointData, null, 2));
    await logInfo(`✅ Checkpoint saved: ${totalProcessed} rows processed`);
    await logInfo(`📍 Checkpoint file: ${filePath}`);
  } catch (error) {
    await logWarn(`⚠️ Could not save checkpoint: ${error}`);
    // Don't throw - let migration continue
  }
};

/**
 * Load checkpoint with better error handling and path fixing
 */
export const loadCheckpoint = async (
  checkpointFile: string,
): Promise<CheckpointData> => {
  try {
    let filePath = normalizeCheckpointPath(checkpointFile);

    const data = await fs.readFile(filePath, "utf8");
    const checkpoint = JSON.parse(data);

    await logInfo(`📋 Loaded checkpoint from: ${filePath}`);
    await logInfo(
      `📊 Previous progress: ${checkpoint.totalProcessed} rows processed`,
    );

    // Show table progress if available
    if (checkpoint.tableProgress) {
      const tableCount = Object.keys(checkpoint.tableProgress).length;
      const completedTables = Object.values(checkpoint.tableProgress).filter(
        (p: any) => p.isComplete,
      ).length;
      await logInfo(
        `📋 Table progress: ${completedTables}/${tableCount} tables completed`,
      );
    }

    return checkpoint;
  } catch (error) {
    await logInfo(`📋 No existing checkpoint found, starting fresh migration`);
    return {
      lastProcessedId: 0,
      totalProcessed: 0,
      timestamp: new Date().toISOString(),
      tableProgress: {},
    };
  }
};

/**
 * Normalize checkpoint path to ensure it's a proper .json file
 */
function normalizeCheckpointPath(checkpointFile: string): string {
  // Handle various input formats
  let filePath = checkpointFile.trim();

  // If it's just a directory or ends with /, add the filename
  if (filePath.endsWith("/") || !extname(filePath)) {
    if (filePath.endsWith("/")) {
      filePath = filePath.slice(0, -1); // Remove trailing slash
    }
    filePath = resolve(filePath, "migration_checkpoint.json");
  } else {
    // Ensure it has .json extension
    if (!filePath.endsWith(".json")) {
      filePath += ".json";
    }
    filePath = resolve(filePath);
  }

  return filePath;
}

/**
 * Mark table as complete in checkpoint
 */
export const markTableComplete = async (
  checkpointFile: string,
  tableId: string,
  totalProcessed: number,
): Promise<void> => {
  try {
    const filePath = normalizeCheckpointPath(checkpointFile);

    let checkpointData: CheckpointData;
    try {
      const existingData = await fs.readFile(filePath, "utf8");
      checkpointData = JSON.parse(existingData);
    } catch {
      return; // No checkpoint file, nothing to mark
    }

    if (!checkpointData.tableProgress) {
      checkpointData.tableProgress = {};
    }

    if (checkpointData.tableProgress[tableId]) {
      checkpointData.tableProgress[tableId].isComplete = true;
      checkpointData.tableProgress[tableId].totalProcessed = totalProcessed;
      checkpointData.timestamp = new Date().toISOString();

      await fs.writeFile(filePath, JSON.stringify(checkpointData, null, 2));
      await logInfo(
        `✅ Marked table ${tableId} as complete (${totalProcessed} rows)`,
      );
    }
  } catch (error) {
    await logWarn(`⚠️ Could not mark table complete: ${error}`);
  }
};

/**
 * Check if table is already complete
 */
export const isTableComplete = async (
  checkpointFile: string,
  tableId: string,
): Promise<boolean> => {
  try {
    const checkpoint = await loadCheckpoint(checkpointFile);
    return checkpoint.tableProgress?.[tableId]?.isComplete || false;
  } catch {
    return false;
  }
};

/**
 * Get table progress
 */
export const getTableProgress = async (
  checkpointFile: string,
  tableId: string,
): Promise<{
  lastProcessedId: number;
  totalProcessed: number;
  lastProcessedRecord?: any;
} | null> => {
  try {
    const checkpoint = await loadCheckpoint(checkpointFile);
    const progress = checkpoint.tableProgress?.[tableId];

    if (progress) {
      return {
        lastProcessedId: progress.lastProcessedId,
        totalProcessed: progress.totalProcessed,
        lastProcessedRecord: progress.lastProcessedRecord,
      };
    }

    return null;
  } catch {
    return null;
  }
};

/**
 * Clear checkpoint (for fresh start)
 */
export const clearCheckpoint = async (
  checkpointFile: string,
): Promise<void> => {
  try {
    const filePath = normalizeCheckpointPath(checkpointFile);
    await fs.unlink(filePath);
    await logInfo(`🗑️ Cleared checkpoint file: ${filePath}`);
  } catch (error) {
    // File might not exist, that's okay
    await logInfo(`📋 No checkpoint file to clear`);
  }
};
