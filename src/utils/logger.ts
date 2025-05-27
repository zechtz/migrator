import { promises as fs } from "fs";

export const log = async (
  level: string,
  message: string,
  logFile: string = "migration.log",
): Promise<void> => {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} - ${level.toUpperCase()} - ${message}\n`;

  console.log(logMessage.trim());

  try {
    await fs.appendFile(logFile, logMessage);
  } catch (err) {
    console.error("Failed to write to log file:", (err as Error).message);
  }
};

export const logInfo = (message: string) => log("info", message);
export const logWarn = (message: string) => log("warn", message);
export const logError = (message: string) => log("error", message);
