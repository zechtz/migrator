/**
 * Data transformation helper functions
 * Shared utilities for converting and validating data types across transformers
 */

/**
 * Safely converts a value to a string, handling null/undefined cases
 * @param value - The value to convert
 * @param maxLength - Optional maximum length to truncate the string
 * @returns String value or null if input is null/undefined/empty
 */
export const safeString = (value: any, maxLength?: number): string | null => {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const str = String(value).trim();
  return maxLength ? str.substring(0, maxLength) : str;
};

/**
 * Safely converts a value to an integer
 * @param value - The value to convert
 * @returns Integer value or null if conversion fails
 */
export const safeInteger = (value: any): number | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const num = parseInt(String(value), 10);
  return isNaN(num) ? null : num;
};

/**
 * Safely converts a value to a float/decimal number
 * @param value - The value to convert
 * @returns Float value or null if conversion fails
 */
export const safeFloat = (value: any): number | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const num = parseFloat(String(value));
  return isNaN(num) ? null : num;
};

/**
 * Safely converts a value to a Date object
 * @param value - The value to convert (Date, string, or number)
 * @returns Date object or null if conversion fails
 */
export const safeDate = (value: any): Date | null => {
  if (value === null || value === undefined) {
    return null;
  }
  if (value instanceof Date) {
    return value;
  }
  const date = new Date(value);
  return isNaN(date.getTime()) ? null : date;
};

/**
 * Safely converts a value to a boolean
 * @param value - The value to convert
 * @returns Boolean value (false for null/undefined/empty)
 */
export const safeBoolean = (value: any): boolean => {
  if (value === null || value === undefined || value === "") {
    return false;
  }
  return Boolean(value);
};

/**
 * Safely converts a value to JSON string for JSONB columns
 * @param value - The value to convert to JSON
 * @returns JSON string or null if value is null/undefined
 */
export const safeJson = (value: any): string | null => {
  if (value === null || value === undefined) {
    return null;
  }
  try {
    return JSON.stringify(value);
  } catch (error) {
    console.warn("Failed to stringify value to JSON:", error);
    return null;
  }
};

/**
 * Safely trims and normalizes string values, converting empty strings to null
 * @param value - The string value to normalize
 * @param maxLength - Optional maximum length
 * @returns Normalized string or null
 */
export const normalizeString = (
  value: any,
  maxLength?: number,
): string | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const str = String(value).trim();
  if (str === "") {
    return null;
  }
  return maxLength ? str.substring(0, maxLength) : str;
};

/**
 * Safely converts a value with a default fallback
 * @param value - The value to convert
 * @param defaultValue - The default value to use if conversion fails
 * @param converter - The conversion function to apply
 * @returns Converted value or default value
 */
export const safeWithDefault = <T>(
  value: any,
  defaultValue: T,
  converter: (val: any) => T | null,
): T => {
  const result = converter(value);
  return result !== null ? result : defaultValue;
};

/**
 * Creates a safe converter with default value for common use cases
 */
export const createSafeConverter = <T>(
  converter: (val: any) => T | null,
  defaultValue: T,
) => {
  return (value: any): T => safeWithDefault(value, defaultValue, converter);
};

// Pre-built converters with common defaults
export const safeIntegerWithDefault = (defaultValue: number = 0) =>
  createSafeConverter(safeInteger, defaultValue);

export const safeDateWithDefault = (defaultValue: Date = new Date()) =>
  createSafeConverter(safeDate, defaultValue);

export const safeStringWithDefault = (defaultValue: string = "") =>
  createSafeConverter(safeString, defaultValue);
