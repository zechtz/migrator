import { Pool } from "pg";
import { logInfo, logError, logWarn } from "../utils/logger.js";
import {
  saveCheckpoint,
  isTableComplete,
  getTableProgress,
  markTableComplete,
} from "../data/checkpoint.js";

export type MigrationResumeStrategy =
  | "skip-existing" // Skip records that already exist (fastest)
  | "upsert" // Update existing, insert new
  | "fresh-start" // Clear table and start over
  | "append-only"; // Just append (original behavior)

export interface MigrationOptions {
  resumeStrategy?: MigrationResumeStrategy;
  uniqueColumns?: string[]; // Columns to check for duplicates
  batchSize?: number;
  checkExistingRecords?: boolean;
}

export class EnhancedMigrationManager {
  constructor(
    private pgPool: Pool,
    private checkpointFile: string,
  ) {}

  /**
   * Check what records already exist in destination table
   */
  async getExistingRecordIds(
    tableName: string,
    uniqueColumns: string[],
    sourceRecords: any[],
  ): Promise<Set<string>> {
    if (sourceRecords.length === 0) return new Set();

    const client = await this.pgPool.connect();

    try {
      // Build a query to check which records already exist
      const keys = sourceRecords.map((record) =>
        uniqueColumns.map((col) => record[col]).join("|"),
      );

      if (keys.length === 0) return new Set();

      // Create a temporary table with the keys to check
      const tempTableName = `temp_check_${Date.now()}`;

      await client.query(`
        CREATE TEMP TABLE ${tempTableName} (
          check_key TEXT PRIMARY KEY
        )
      `);

      // Insert all keys to check
      for (const key of keys) {
        await client.query(
          `INSERT INTO ${tempTableName} (check_key) VALUES ($1) ON CONFLICT DO NOTHING`,
          [key],
        );
      }

      // Find which keys already exist in the destination table
      const whereClause = uniqueColumns
        .map((col, idx) => `${col} = split_part(t.check_key, '|', ${idx + 1})`)
        .join(" AND ");

      const existingQuery = `
        SELECT t.check_key
        FROM ${tempTableName} t
        WHERE EXISTS (
          SELECT 1 FROM ${tableName} d
          WHERE ${whereClause}
        )
      `;

      const result = await client.query(existingQuery);

      await client.query(`DROP TABLE ${tempTableName}`);

      return new Set(result.rows.map((row) => row.check_key));
    } finally {
      client.release();
    }
  }

  /**
   * Enhanced insertion with duplicate prevention
   */
  async insertWithDuplicatePrevention(
    tableName: string,
    records: any[],
    options: MigrationOptions,
  ): Promise<{ inserted: number; skipped: number; updated: number }> {
    if (records.length === 0) {
      return { inserted: 0, skipped: 0, updated: 0 };
    }

    const {
      resumeStrategy = "skip-existing",
      uniqueColumns = ["provided_pin_no"],
    } = options;

    let inserted = 0;
    let skipped = 0;
    let updated = 0;

    switch (resumeStrategy) {
      case "fresh-start":
        // Clear table first
        await this.clearTable(tableName);
        return await this.bulkInsert(tableName, records);

      case "append-only":
        // Just insert everything (original behavior)
        return await this.bulkInsert(tableName, records);

      case "skip-existing":
        // Check for existing records and skip them
        const existingKeys = await this.getExistingRecordIds(
          tableName,
          uniqueColumns,
          records,
        );

        const newRecords = records.filter((record) => {
          const key = uniqueColumns.map((col) => record[col]).join("|");
          return !existingKeys.has(key);
        });

        skipped = records.length - newRecords.length;

        if (newRecords.length > 0) {
          const result = await this.bulkInsert(tableName, newRecords);
          inserted = result.inserted;
        }

        await logInfo(
          `📊 Duplicate check: ${inserted} new, ${skipped} skipped`,
        );
        return { inserted, skipped, updated: 0 };

      case "upsert":
        // Use upsert for all records
        return await this.bulkUpsert(tableName, records, uniqueColumns);

      default:
        throw new Error(`Unknown resume strategy: ${resumeStrategy}`);
    }
  }

  /**
   * Bulk insert records
   */
  private async bulkInsert(
    tableName: string,
    records: any[],
  ): Promise<{ inserted: number; skipped: number; updated: number }> {
    if (records.length === 0) return { inserted: 0, skipped: 0, updated: 0 };

    const client = await this.pgPool.connect();

    try {
      await client.query("BEGIN");

      const columns = Object.keys(records[0]);
      const values = records.map((record) => Object.values(record));

      const placeholders = records
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
      await client.query("COMMIT");

      return { inserted: result.rowCount || 0, skipped: 0, updated: 0 };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Bulk upsert records
   */
  private async bulkUpsert(
    tableName: string,
    records: any[],
    uniqueColumns: string[],
  ): Promise<{ inserted: number; skipped: number; updated: number }> {
    if (records.length === 0) return { inserted: 0, skipped: 0, updated: 0 };

    const client = await this.pgPool.connect();
    let inserted = 0;
    let updated = 0;

    try {
      await client.query("BEGIN");

      for (const record of records) {
        const columns = Object.keys(record);
        const values = Object.values(record);
        const placeholders = values.map((_, idx) => `$${idx + 1}`).join(", ");

        // Build conflict resolution
        const conflictTarget = uniqueColumns.join(", ");
        const updateColumns = columns.filter(
          (col) => !uniqueColumns.includes(col),
        );
        const updateSet = updateColumns
          .map((col) => `${col} = EXCLUDED.${col}`)
          .join(", ");

        const query = `
          INSERT INTO ${tableName} (${columns.join(", ")})
          VALUES (${placeholders})
          ON CONFLICT (${conflictTarget})
          DO UPDATE SET ${updateSet}
          RETURNING (xmax = 0) AS inserted
        `;

        const result = await client.query(query, values);

        if (result.rows[0]?.inserted) {
          inserted++;
        } else {
          updated++;
        }
      }

      await client.query("COMMIT");
      return { inserted, skipped: 0, updated };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Clear table
   */
  private async clearTable(tableName: string): Promise<void> {
    const client = await this.pgPool.connect();

    try {
      const result = await client.query(`DELETE FROM ${tableName}`);
      await logInfo(
        `🗑️ Cleared ${result.rowCount || 0} existing records from ${tableName}`,
      );
    } finally {
      client.release();
    }
  }

  /**
   * Check migration status for a table
   */
  async checkMigrationStatus(
    tableId: string,
    targetTable: string,
  ): Promise<{
    isComplete: boolean;
    recordsInDestination: number;
    lastCheckpoint?: any;
  }> {
    const isComplete = await isTableComplete(this.checkpointFile, tableId);
    const lastCheckpoint = await getTableProgress(this.checkpointFile, tableId);

    // Count records in destination table
    const client = await this.pgPool.connect();
    try {
      const result = await client.query(`SELECT COUNT(*) FROM ${targetTable}`);
      const recordsInDestination = parseInt(result.rows[0].count);

      return {
        isComplete,
        recordsInDestination,
        lastCheckpoint,
      };
    } finally {
      client.release();
    }
  }

  /**
   * Resume migration with smart duplicate handling
   */
  async resumeMigration(
    tableId: string,
    targetTable: string,
    options: MigrationOptions,
  ): Promise<boolean> {
    const status = await this.checkMigrationStatus(tableId, targetTable);

    await logInfo(`📊 Migration status for ${tableId}:`);
    await logInfo(`   Complete: ${status.isComplete}`);
    await logInfo(`   Records in destination: ${status.recordsInDestination}`);

    if (status.lastCheckpoint) {
      await logInfo(
        `   Last processed: ${status.lastCheckpoint.totalProcessed} records`,
      );
    }

    if (status.isComplete) {
      await logInfo(`✅ Table ${tableId} already completed, skipping`);
      return false; // Don't run migration
    }

    if (
      status.recordsInDestination > 0 &&
      options.resumeStrategy === "skip-existing"
    ) {
      await logInfo(`🔄 Resuming migration with duplicate prevention`);
      return true; // Run migration with duplicate checking
    }

    if (
      status.recordsInDestination > 0 &&
      options.resumeStrategy === "fresh-start"
    ) {
      await logWarn(
        `⚠️ Fresh start requested - will clear ${status.recordsInDestination} existing records`,
      );
      return true; // Run migration with table clearing
    }

    return true; // Run migration normally
  }
}

/**
 * Enhanced migration function with duplicate prevention
 */
export const migrateTableEnhanced = async (
  connections: any,
  config: any,
  sourceQuery: string,
  targetTable: string,
  tableId: string,
  transformFn?: any,
  options: MigrationOptions = {},
): Promise<number> => {
  const manager = new EnhancedMigrationManager(
    connections.postgresPool,
    config.checkpointFile,
  );

  // Set default options
  const migrationOptions: MigrationOptions = {
    resumeStrategy: "skip-existing",
    uniqueColumns: ["provided_pin_no"],
    batchSize: config.batchSize || 1000,
    checkExistingRecords: true,
    ...options,
  };

  await logInfo(`🚀 Starting enhanced migration for ${targetTable}`);
  await logInfo(`📋 Resume strategy: ${migrationOptions.resumeStrategy}`);
  await logInfo(
    `🔑 Unique columns: ${migrationOptions.uniqueColumns?.join(", ")}`,
  );

  // Check if we should resume
  const shouldRun = await manager.resumeMigration(
    tableId,
    targetTable,
    migrationOptions,
  );
  if (!shouldRun) {
    return 0; // Table already completed
  }

  // Get existing progress
  const progress = await getTableProgress(config.checkpointFile, tableId);
  let offset = progress?.lastProcessedId || 0;
  let totalProcessed = progress?.totalProcessed || 0;
  let batchCount = 0;

  while (true) {
    try {
      await logInfo(`📦 Processing batch ${batchCount + 1}, offset: ${offset}`);

      // Fetch data from Oracle (using existing Oracle fetch logic)
      const { fetchOracleData } = await import("../database/oracle.js");
      const oracleData = await fetchOracleData(
        connections.oracle,
        sourceQuery,
        offset,
        migrationOptions.batchSize!,
      );

      if (oracleData.length === 0) {
        await logInfo("✅ No more data to process");
        break;
      }

      // Transform data
      const { transformData } = await import("../data/transform.js");
      const transformedData = await transformData(oracleData, transformFn);

      // Insert with duplicate prevention
      const result = await manager.insertWithDuplicatePrevention(
        targetTable,
        transformedData,
        migrationOptions,
      );

      totalProcessed += result.inserted;
      offset += oracleData.length; // Move offset by source records, not just inserted
      batchCount++;

      await logInfo(
        `📊 Batch ${batchCount}: ${result.inserted} inserted, ${result.skipped} skipped, ${result.updated} updated`,
      );

      // Save checkpoint with last processed record info
      const lastRecord = transformedData[transformedData.length - 1];
      await saveCheckpoint(
        config.checkpointFile,
        offset,
        totalProcessed,
        tableId,
        lastRecord,
      );

      // Small delay to prevent overwhelming the databases
      await new Promise((resolve) => setTimeout(resolve, 100));
    } catch (error) {
      await logError(`❌ Batch ${batchCount + 1} failed: ${error}`);
      throw error;
    }
  }

  // Mark table as complete
  await markTableComplete(config.checkpointFile, tableId, totalProcessed);
  await logInfo(
    `✅ Enhanced migration completed: ${totalProcessed} total records processed`,
  );

  return totalProcessed;
};
