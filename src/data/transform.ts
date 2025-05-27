// Type for transform functions
export type TransformFunction = (row: any) => Record<string, any>;

// Generic transform data function that accepts a custom transformer
export const transformData = async (
  oracleData: any[],
  transformFn?: TransformFunction,
): Promise<Record<string, any>[]> => {
  if (transformFn) {
    return oracleData.map(transformFn);
  }

  // Default transformation (basic type conversion)
  return defaultTransform(oracleData);
};

// Default transformation function (the original logic)
export const defaultTransform = (oracleData: any[]): Record<string, any>[] => {
  const transformedData: Record<string, any>[] = [];

  for (const row of oracleData) {
    const transformedRow: Record<string, any> = {};

    for (const [key, value] of Object.entries(row)) {
      const lowerKey = key.toLowerCase();

      if (value === null || value === undefined) {
        transformedRow[lowerKey] = null;
      } else if (value instanceof Date) {
        transformedRow[lowerKey] = value;
      } else if (typeof value === "number") {
        transformedRow[lowerKey] = value;
      } else if (Buffer.isBuffer(value)) {
        transformedRow[lowerKey] = value.toString();
      } else {
        transformedRow[lowerKey] = String(value);
      }
    }

    transformedData.push(transformedRow);
  }

  return transformedData;
};

// Helper functions for common transformations
export const createFieldMapper = (
  fieldMappings: Record<string, string>,
): TransformFunction => {
  return (row: any) => {
    const transformed: Record<string, any> = {};

    for (const [oracleField, postgresField] of Object.entries(fieldMappings)) {
      transformed[postgresField] = row[oracleField];
    }

    return transformed;
  };
};

export const createCustomTransformer = (
  transformLogic: (row: any) => Record<string, any>,
): TransformFunction => {
  return transformLogic;
};

// Common transform patterns
export const transforms = {
  // Just lowercase all field names
  lowercaseFields: (row: any) => {
    const transformed: Record<string, any> = {};
    for (const [key, value] of Object.entries(row)) {
      transformed[key.toLowerCase()] = value;
    }
    return transformed;
  },

  // Apply field mappings and basic type conversion
  mapAndConvert: (fieldMappings: Record<string, string>) => (row: any) => {
    const transformed: Record<string, any> = {};

    for (const [oracleField, postgresField] of Object.entries(fieldMappings)) {
      let value = row[oracleField];

      // Basic type conversion
      if (value === null || value === undefined) {
        value = null;
      } else if (typeof value === "string") {
        value = value.trim();
      } else if (Buffer.isBuffer(value)) {
        value = value.toString();
      }

      transformed[postgresField] = value;
    }

    return transformed;
  },

  // Add computed fields
  withComputedFields:
    (computedFields: Record<string, (row: any) => any>) => (row: any) => {
      const transformed = { ...row };

      for (const [fieldName, computeFn] of Object.entries(computedFields)) {
        transformed[fieldName] = computeFn(row);
      }

      return transformed;
    },
};
