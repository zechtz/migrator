import { PoolClient } from "pg";
import { logInfo, logError } from "./logger.js";

export interface OperationResult {
  inserted?: number;
  updated?: number;
  skipped?: number;
  deleted?: number;
}

/**
 * Insert data with conflict resolution (UPSERT)
 */
export const upsertData = async (
  client: PoolClient,
  tableName: string,
  data: any[],
  conflictColumns: string[],
  options: {
    onConflict?: "update" | "ignore";
    updateColumns?: string[];
    updateConditions?: string;
  } = {},
): Promise<OperationResult> => {
  if (data.length === 0) return { inserted: 0, updated: 0 };

  const { onConflict = "update", updateColumns, updateConditions } = options;

  let inserted = 0;
  let updated = 0;

  for (const row of data) {
    try {
      const columns = Object.keys(row);
      const values = Object.values(row);
      const placeholders = values.map((_, idx) => `$${idx + 1}`).join(", ");

      // Build conflict resolution
      const conflictTarget = conflictColumns.join(", ");
      let conflictAction = "";

      if (onConflict === "update") {
        const columnsToUpdate =
          updateColumns ||
          columns.filter((col) => !conflictColumns.includes(col));
        let updateSet = columnsToUpdate
          .map((col) => `${col} = EXCLUDED.${col}`)
          .join(", ");

        // Add update conditions if specified
        let whereClause = "";
        if (updateConditions) {
          whereClause = ` WHERE ${updateConditions}`;
        }

        conflictAction = `DO UPDATE SET ${updateSet}${whereClause}`;
      } else {
        conflictAction = "DO NOTHING";
      }

      const query = `
        INSERT INTO ${tableName} (${columns.join(", ")})
        VALUES (${placeholders})
        ON CONFLICT (${conflictTarget})
        ${conflictAction}
        RETURNING (xmax = 0) AS inserted
      `;

      const result = await client.query(query, values);

      if (result.rows[0]?.inserted) {
        inserted++;
      } else {
        updated++;
      }
    } catch (error) {
      await logError(`Upsert failed for row: ${error}`);
      throw error;
    }
  }

  return { inserted, updated };
};

/**
 * Update existing records only
 */
export const updateData = async (
  client: PoolClient,
  tableName: string,
  data: any[],
  keyColumns: string[],
  options: {
    updateColumns?: string[];
    updateConditions?: string;
  } = {},
): Promise<OperationResult> => {
  if (data.length === 0) return { updated: 0, skipped: 0 };

  const { updateColumns, updateConditions } = options;
  let updated = 0;
  let skipped = 0;

  for (const row of data) {
    try {
      // Build WHERE clause from key columns
      const whereClause = keyColumns
        .map((col, idx) => `${col} = $${idx + 1}`)
        .join(" AND ");

      const keyValues = keyColumns.map((col) => row[col]);

      // Build SET clause
      const columnsToUpdate =
        updateColumns ||
        Object.keys(row).filter((col) => !keyColumns.includes(col));

      const setClause = columnsToUpdate
        .map((col, idx) => `${col} = $${keyColumns.length + idx + 1}`)
        .join(", ");

      const updateValues = columnsToUpdate.map((col) => row[col]);

      // Build complete WHERE clause with additional conditions
      let completeWhereClause = whereClause;
      if (updateConditions) {
        completeWhereClause += ` AND (${updateConditions})`;
      }

      const query = `
        UPDATE ${tableName} 
        SET ${setClause}
        WHERE ${completeWhereClause}
      `;

      const result = await client.query(query, [...keyValues, ...updateValues]);

      if (result.rowCount && result.rowCount > 0) {
        updated++;
      } else {
        skipped++;
      }
    } catch (error) {
      await logError(`Update failed for row: ${error}`);
      throw error;
    }
  }

  return { updated, skipped };
};

/**
 * Delete all records then insert fresh data
 */
export const deleteAndInsert = async (
  client: PoolClient,
  tableName: string,
  data: any[],
  deleteConditions?: string,
): Promise<OperationResult> => {
  let deleted = 0;
  let inserted = 0;

  try {
    // Delete existing records
    const deleteQuery = deleteConditions
      ? `DELETE FROM ${tableName} WHERE ${deleteConditions}`
      : `DELETE FROM ${tableName}`;

    const deleteResult = await client.query(deleteQuery);
    deleted = deleteResult.rowCount || 0;

    // Insert new data
    if (data.length > 0) {
      const insertResult = await insertData(client, tableName, data);
      inserted = insertResult.inserted || 0;
    }

    return { deleted, inserted };
  } catch (error) {
    await logError(`Delete and insert failed: ${error}`);
    throw error;
  }
};

/**
 * Standard insert operation
 */
export const insertData = async (
  client: PoolClient,
  tableName: string,
  data: any[],
): Promise<OperationResult> => {
  if (data.length === 0) return { inserted: 0 };

  try {
    const columns = Object.keys(data[0]);
    const values = data.map((row) => Object.values(row));

    const placeholders = data
      .map(
        (_, rowIdx) =>
          `(${columns.map((_, colIdx) => `$${rowIdx * columns.length + colIdx + 1}`).join(", ")})`,
      )
      .join(", ");

    const query = `
      INSERT INTO ${tableName} (${columns.join(", ")})
      VALUES ${placeholders}
    `;

    const result = await client.query(query, values.flat());
    return { inserted: result.rowCount || 0 };
  } catch (error) {
    await logError(`Insert failed: ${error}`);
    throw error;
  }
};

/**
 * Execute the appropriate database operation based on migration mode
 */
export const executeOperation = async (
  client: PoolClient,
  tableName: string,
  data: any[],
  mode: "insert" | "upsert" | "update" | "delete-insert" = "insert",
  options: {
    conflictColumns?: string[];
    updateColumns?: string[];
    updateConditions?: string;
    onConflict?: "update" | "ignore";
    deleteConditions?: string;
  } = {},
): Promise<OperationResult> => {
  await logInfo(
    `Executing ${mode} operation on ${tableName} with ${data.length} records`,
  );

  switch (mode) {
    case "insert":
      return await insertData(client, tableName, data);

    case "upsert":
      if (!options.conflictColumns || options.conflictColumns.length === 0) {
        throw new Error("conflictColumns required for upsert operation");
      }
      return await upsertData(
        client,
        tableName,
        data,
        options.conflictColumns,
        options,
      );

    case "update":
      if (!options.conflictColumns || options.conflictColumns.length === 0) {
        throw new Error("conflictColumns required for update operation");
      }
      return await updateData(
        client,
        tableName,
        data,
        options.conflictColumns,
        options,
      );

    case "delete-insert":
      return await deleteAndInsert(
        client,
        tableName,
        data,
        options.deleteConditions,
      );

    default:
      throw new Error(`Unknown migration mode: ${mode}`);
  }
};
