import { Pool, PoolClient } from "pg";
import { PostgresConfig } from "types";
import { logInfo, logError } from "utils/logger";

export const connectToPostgres = async (
  config: PostgresConfig,
): Promise<Pool> => {
  try {
    const pool = new Pool(config);

    // Test connection
    const client = await pool.connect();
    await client.query("SELECT NOW()");
    client.release();

    await logInfo("Connected to PostgreSQL database");
    return pool;
  } catch (error) {
    await logError(
      `Failed to connect to PostgreSQL: ${(error as Error).message}`,
    );
    throw error;
  }
};

export const insertPostgresData = async (
  pool: Pool,
  data: Record<string, any>[],
  tableName: string,
): Promise<void> => {
  if (!data || data.length === 0) {
    return;
  }

  const client: PoolClient = await pool.connect();

  try {
    await client.query("BEGIN");

    const columns = Object.keys(data[0]);
    const placeholders = columns.map((_, i) => `$${i + 1}`).join(", ");

    const insertQuery = `
            INSERT INTO ${tableName} (${columns.join(", ")})
            VALUES (${placeholders})
            ON CONFLICT DO NOTHING
        `;

    for (const row of data) {
      const values = columns.map((col) => row[col]);
      await client.query(insertQuery, values);
    }

    await client.query("COMMIT");
    await logInfo(`Inserted ${data.length} rows into ${tableName}`);
  } catch (error) {
    await client.query("ROLLBACK");
    await logError(`Error inserting data: ${(error as Error).message}`);
    throw error;
  } finally {
    client.release();
  }
};

export const closePostgresPool = async (pool: Pool): Promise<void> => {
  try {
    await pool.end();
    await logInfo("PostgreSQL pool closed");
  } catch (error) {
    await logError(
      `Error closing PostgreSQL pool: ${(error as Error).message}`,
    );
  }
};
