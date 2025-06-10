import {
  TransformFunction,
  EnhancedTransformFunction,
} from "../types/index.js";
import { ForeignKeyResolver } from "../utils/foreign-key-resolver.js";
import { logWarn } from "../utils/logger.js";

export const transformData = async (
  rows: any[],
  transformFn?: TransformFunction | EnhancedTransformFunction,
  resolvers?: Record<string, ForeignKeyResolver>,
): Promise<any[]> => {
  if (!transformFn) {
    return rows; // No transformation needed
  }

  const transformedRows: any[] = [];

  for (const row of rows) {
    try {
      let transformedRow;

      // Check if it's an enhanced transform function (async and takes resolvers)
      if (isEnhancedTransformFunction(transformFn)) {
        // Call the enhanced function with resolvers
        const result = transformFn(row, resolvers);
        // Handle both sync and async enhanced functions
        transformedRow = result instanceof Promise ? await result : result;
      } else {
        // Legacy transform function (sync, no resolvers)
        transformedRow = (transformFn as TransformFunction)(row);
      }

      transformedRows.push(transformedRow);
    } catch (error) {
      await logWarn(`Transform failed for row: ${error}`);
      throw error;
    }
  }

  return transformedRows;
};

/**
 * Type guard to check if transform function is enhanced
 */
function isEnhancedTransformFunction(
  fn: TransformFunction | EnhancedTransformFunction,
): fn is EnhancedTransformFunction {
  // Check if function accepts more than 1 parameter
  return fn.length > 1;
}

/**
 * Backward compatibility
 */
export const transformDataLegacy = async (
  rows: any[],
  transformFn?: TransformFunction,
): Promise<any[]> => {
  return transformData(rows, transformFn);
};
