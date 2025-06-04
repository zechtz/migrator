import { Pool, PoolClient } from "pg";
import { logWarn, logError } from "../utils/logger.js";
import { MigrationTask, TransformFunction } from "../types/index.js";

export type MigrationMode = "insert" | "update" | "upsert" | "sync";

export interface UpdateMigrationTask
  extends Omit<MigrationTask, "transformFn"> {
  mode: MigrationMode;

  // For updates/upserts - specify key columns to match on
  keyColumns: string[];

  // For updates - specify which columns to update
  updateColumns?: string[];

  // For conditional updates
  updateCondition?: string;

  // Transform function (same as before)
  transformFn?: TransformFunction;

  // Custom conflict resolution for upserts
  conflictResolution?: "update" | "ignore" | "error" | "undefined";

  // Batch update strategy
  updateStrategy?: "batch" | "individual";
}

export class UpdateMigrationHandler {
  constructor(private pgPool: Pool) {}

  /**
   * Strategy 1: Pure UPDATE - only updates existing records
   */
  async handleUpdate(
    client: PoolClient,
    tableName: string,
    data: any[],
    keyColumns: string[],
    updateColumns?: string[],
  ): Promise<{ updated: number; skipped: number }> {
    let updated = 0;
    let skipped = 0;

    for (const row of data) {
      try {
        // Build WHERE clause from key columns
        const whereClause = keyColumns
          .map((col, idx) => `${col} = $${idx + 1}`)
          .join(" AND ");

        const keyValues = keyColumns.map((col) => row[col]);

        // Build SET clause - either specified columns or all except keys
        const columnsToUpdate =
          updateColumns ||
          Object.keys(row).filter((col) => !keyColumns.includes(col));

        const setClause = columnsToUpdate
          .map((col, idx) => `${col} = $${keyColumns.length + idx + 1}`)
          .join(", ");

        const updateValues = columnsToUpdate.map((col) => row[col]);

        const query = `
          UPDATE ${tableName} 
          SET ${setClause}, updated_at = CURRENT_TIMESTAMP
          WHERE ${whereClause}
        `;

        const result = await client.query(query, [
          ...keyValues,
          ...updateValues,
        ]);

        if (result.rowCount && result.rowCount > 0) {
          updated++;
        } else {
          skipped++;
          logWarn(
            `No matching record found for update: ${JSON.stringify(keyValues)}`,
          );
        }
      } catch (error) {
        logError(`Update failed for row: ${error}`);
        throw error;
      }
    }

    return { updated, skipped };
  }

  /**
   * Strategy 2: UPSERT - insert if not exists, update if exists
   */
  async handleUpsert(
    client: PoolClient,
    tableName: string,
    data: any[],
    keyColumns: string[],
    conflictResolution: "update" | "ignore" = "update",
  ): Promise<{ inserted: number; updated: number }> {
    let inserted = 0;
    let updated = 0;

    for (const row of data) {
      try {
        const columns = Object.keys(row);
        const values = Object.values(row);
        const placeholders = values.map((_, idx) => `$${idx + 1}`).join(", ");

        // Build conflict resolution
        const conflictTarget = keyColumns.join(", ");
        let conflictAction = "";

        if (conflictResolution === "update") {
          const updateColumns = columns.filter(
            (col) => !keyColumns.includes(col),
          );
          const updateSet = updateColumns
            .map((col) => `${col} = EXCLUDED.${col}`)
            .join(", ");
          conflictAction = `DO UPDATE SET ${updateSet}, updated_at = CURRENT_TIMESTAMP`;
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
        logError(`Upsert failed for row: ${error}`);
        throw error;
      }
    }

    return { inserted, updated };
  }

  /**
   * Strategy 3: MERGE/SYNC - comprehensive synchronization
   */
  async handleSync(
    client: PoolClient,
    tableName: string,
    data: any[],
    keyColumns: string[],
  ): Promise<{ inserted: number; updated: number; deleted: number }> {
    let stats = { inserted: 0, updated: 0, deleted: 0 };

    // Step 1: Get existing records
    const keyPlaceholders = data
      .map(
        (_, idx) =>
          `(${keyColumns.map((_, colIdx) => `$${idx * keyColumns.length + colIdx + 1}`).join(", ")})`,
      )
      .join(", ");

    const keyValues = data.flatMap((row) => keyColumns.map((col) => row[col]));

    const existingQuery = `
      SELECT ${keyColumns.join(", ")} 
      FROM ${tableName} 
      WHERE (${keyColumns.join(", ")}) IN (VALUES ${keyPlaceholders})
    `;

    const existingResult = await client.query(existingQuery, keyValues);
    const existingKeys = new Set(
      existingResult.rows.map((row) =>
        keyColumns.map((col) => row[col]).join("|"),
      ),
    );

    // Step 2: Separate inserts and updates
    const toInsert: any[] = [];
    const toUpdate: any[] = [];

    for (const row of data) {
      const rowKey = keyColumns.map((col) => row[col]).join("|");
      if (existingKeys.has(rowKey)) {
        toUpdate.push(row);
      } else {
        toInsert.push(row);
      }
    }

    // Step 3: Perform operations
    if (toInsert.length > 0) {
      const insertResult = await this.bulkInsert(client, tableName, toInsert);
      stats.inserted = insertResult.inserted;
    }

    if (toUpdate.length > 0) {
      const updateResult = await this.handleUpdate(
        client,
        tableName,
        toUpdate,
        keyColumns,
      );
      stats.updated = updateResult.updated;
    }

    return stats;
  }

  /**
   * Optimized batch upsert using temporary tables
   */
  async handleBatchUpsert(
    client: PoolClient,
    tableName: string,
    data: any[],
    keyColumns: string[],
  ): Promise<{ inserted: number; updated: number }> {
    const tempTableName = `temp_${tableName.replace(".", "_")}_${Date.now()}`;

    try {
      // Create temporary table with same structure
      await client.query(`
        CREATE TEMP TABLE ${tempTableName} 
        (LIKE ${tableName} INCLUDING DEFAULTS)
      `);

      // Insert all data into temp table
      await this.bulkInsert(client, tempTableName, data);

      // Perform upsert from temp table
      const columns = Object.keys(data[0]);
      const updateColumns = columns.filter((col) => !keyColumns.includes(col));

      const conflictTarget = keyColumns.join(", ");
      const updateSet = updateColumns
        .map((col) => `${col} = EXCLUDED.${col}`)
        .join(", ");

      const upsertQuery = `
        INSERT INTO ${tableName} (${columns.join(", ")})
        SELECT ${columns.join(", ")} FROM ${tempTableName}
        ON CONFLICT (${conflictTarget})
        DO UPDATE SET ${updateSet}, updated_at = CURRENT_TIMESTAMP
        RETURNING (xmax = 0) AS inserted
      `;

      const result = await client.query(upsertQuery);

      const inserted = result.rows.filter((row) => row.inserted).length;
      const updated = result.rows.length - inserted;

      return { inserted, updated };
    } finally {
      // Clean up temp table
      await client.query(`DROP TABLE IF EXISTS ${tempTableName}`);
    }
  }

  private async bulkInsert(
    client: PoolClient,
    tableName: string,
    data: any[],
  ): Promise<{ inserted: number }> {
    if (data.length === 0) return { inserted: 0 };

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
  }
}
