import { promises as fs } from "fs";
import path from "path";
import { Checkpoint } from "../types/index.js";
import { logError, logWarn } from "../utils/logger.js";

export const loadCheckpoint = async (
  checkpointFile: string,
): Promise<Checkpoint> => {
  try {
    const checkpointPath = path.resolve(checkpointFile);
    const data = await fs.readFile(checkpointPath, "utf8");
    return JSON.parse(data);
  } catch (error) {
    await logWarn(`Could not load checkpoint: ${(error as Error).message}`);
    return { lastProcessedId: 0, totalProcessed: 0 };
  }
};

export const saveCheckpoint = async (
  checkpointFile: string,
  lastId: number,
  totalProcessed: number,
): Promise<Checkpoint> => {
  const checkpoint: Checkpoint = {
    lastProcessedId: lastId,
    totalProcessed: totalProcessed,
    lastUpdate: new Date().toISOString(),
  };

  try {
    await fs.writeFile(checkpointFile, JSON.stringify(checkpoint, null, 2));
    return checkpoint;
  } catch (error) {
    await logError(`Failed to save checkpoint: ${(error as Error).message}`);
    throw error;
  }
};
