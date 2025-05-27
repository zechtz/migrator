import { logError, logInfo } from "utils/logger";

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

export const retryOperation = async <T>(
  operation: () => Promise<T>,
  maxRetries: number,
  retryDelay: number,
  operationName: string,
): Promise<T> => {
  let lastError: Error;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error as Error;
      await logError(
        `${operationName} failed (attempt ${attempt}): ${lastError.message}`,
      );

      if (attempt < maxRetries) {
        await logInfo(`Retrying in ${retryDelay}ms...`);
        await sleep(retryDelay);
      }
    }
  }

  await logError(`Max retries reached for ${operationName}. Operation failed.`);
  throw lastError!;
};
