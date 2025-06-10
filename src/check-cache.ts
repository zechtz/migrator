#!/usr/bin/env node

import { createConfig } from "./config/index.js";
import { createConfigFromEnv } from "./config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "./database/connections.js";
import { logInfo, logError } from "./utils/logger.js";

const checkCacheData = async (): Promise<void> => {
  console.log("🔍 Checking Cache Data");
  console.log("====================");
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    const config = createConfig(configOptions);
    connections = await initializeConnections(config);

    const client = await connections.postgresPool.connect();

    try {
      // Check each reference table
      const tables = [
        {
          name: "crvs_global.tbl_delimitation_region",
          codeCol: "code",
          idCol: "id",
        },
        {
          name: "crvs_global.tbl_delimitation_district",
          codeCol: "code",
          idCol: "id",
        },
        {
          name: "crvs_global.tbl_delimitation_council",
          codeCol: "code",
          idCol: "id",
        },
        {
          name: "crvs_global.tbl_delimitation_ward",
          codeCol: "code",
          idCol: "ward_id",
        },
      ];

      for (const table of tables) {
        await logInfo(`\n📋 Checking ${table.name}:`);

        try {
          // Check if table exists
          const existsResult = await client.query(
            `
            SELECT EXISTS (
              SELECT FROM information_schema.tables 
              WHERE table_schema = split_part($1, '.', 1) 
              AND table_name = split_part($1, '.', 2)
            )
          `,
            [table.name],
          );

          if (!existsResult.rows[0].exists) {
            await logError(`❌ Table ${table.name} does not exist`);
            continue;
          }

          // Check table structure
          const columnsResult = await client.query(
            `
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = split_part($1, '.', 1) 
            AND table_name = split_part($1, '.', 2)
            ORDER BY ordinal_position
          `,
            [table.name],
          );

          await logInfo(
            `   📊 Columns: ${columnsResult.rows.map((r: any) => `${r.column_name}(${r.data_type})`).join(", ")}`,
          );

          // Check data count and samples
          const dataResult = await client.query(`
            SELECT 
              COUNT(*) as total_count,
              COUNT(${table.codeCol}) as code_count,
              COUNT(${table.idCol}) as id_count,
              array_agg(${table.codeCol} ORDER BY ${table.idCol} LIMIT 5) as sample_codes,
              array_agg(${table.idCol} ORDER BY ${table.idCol} LIMIT 5) as sample_ids
            FROM ${table.name}
          `);

          const data = dataResult.rows[0];
          await logInfo(`   📊 Total rows: ${data.total_count}`);
          await logInfo(`   📊 Rows with code: ${data.code_count}`);
          await logInfo(`   📊 Rows with ID: ${data.id_count}`);

          if (data.sample_codes && data.sample_codes[0]) {
            await logInfo(
              `   📋 Sample codes: ${data.sample_codes.join(", ")}`,
            );
            await logInfo(`   📋 Sample IDs: ${data.sample_ids.join(", ")}`);
          }
        } catch (error) {
          await logError(`❌ Error checking ${table.name}: ${error}`);
        }
      }
    } finally {
      client.release();
    }
  } catch (error) {
    await logError(`❌ Cache check failed: ${error}`);
    throw error;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

if (require.main === module) {
  checkCacheData().catch((error) => {
    console.error("❌ Cache check fatal error:", error);
    process.exit(1);
  });
}
