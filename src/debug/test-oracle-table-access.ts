#!/usr/bin/env node

import { createConfig } from "../config/index.js";
import { createConfigFromEnv } from "../config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "../database/connections.js";
import { logInfo, logError, logWarn } from "../utils/logger.js";

/**
 * Test each Oracle table individually to find which one is causing ORA-00903
 */
const testOracleTableAccess = async (): Promise<void> => {
  console.log("🔍 Testing Oracle Table Access");
  console.log("=============================");
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    const config = createConfig(configOptions);
    connections = await initializeConnections(config);

    // List of tables used in the birth registration query
    const tables = [
      "CRVS.PERSON",
      "CRVS.BIRTH_REGISTRATION",
      "CRVS.SEX",
      "CRVS.PLACE_OF_BIRTH",
      "CRVS.REGION",
      "CRVS.DISTRICT",
      "CRVS.WARD",
      "CRVS.COUNTRY",
      "CRVS.HOSPITALS",
    ];

    await logInfo("🧪 Testing table access one by one...");

    const existingTables = [];
    const missingTables = [];

    for (const table of tables) {
      try {
        await logInfo(`Testing: ${table}`);

        // Test basic access with simple query
        const testQuery = `SELECT COUNT(*) FROM ${table} WHERE ROWNUM = 1`;
        const result = await connections.oracle.execute(testQuery);

        if (result.rows && result.rows.length > 0) {
          const count = result.rows[0][0];
          await logInfo(`✅ ${table}: Accessible (${count} total rows)`);
          existingTables.push(table);
        }
      } catch (error: any) {
        await logError(`❌ ${table}: ${error.message.split("\n")[0]}`);
        missingTables.push({ table, error: error.message });

        // Try to find alternative table names
        if (
          error.message.includes("ORA-00942") ||
          error.message.includes("ORA-00903")
        ) {
          await logWarn(`   Looking for alternative names for ${table}...`);
          await findAlternativeTableName(connections, table);
        }
      }
    }

    // Summary
    console.log("\n📊 Summary:");
    console.log("==========");
    await logInfo(
      `✅ Accessible tables (${existingTables.length}): ${existingTables.join(", ")}`,
    );

    if (missingTables.length > 0) {
      await logError(
        `❌ Missing/Inaccessible tables (${missingTables.length}):`,
      );
      missingTables.forEach(({ table, error }) => {
        console.log(`   ${table}: ${error.split("\n")[0]}`);
      });
    }

    // Test a simplified query with only accessible tables
    if (
      existingTables.includes("CRVS.PERSON") &&
      existingTables.includes("CRVS.BIRTH_REGISTRATION")
    ) {
      await testSimplifiedQuery(connections, existingTables);
    }
  } catch (error) {
    await logError(`❌ Table access test failed: ${error}`);
    throw error;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

/**
 * Find alternative table names
 */
async function findAlternativeTableName(connections: any, tableName: string) {
  try {
    const tableNameOnly = tableName.split(".")[1];
    const schema = tableName.split(".")[0];

    // Search for similar table names
    const searchQuery = `
      SELECT OWNER, TABLE_NAME 
      FROM ALL_TABLES 
      WHERE UPPER(TABLE_NAME) LIKE '%${tableNameOnly}%' 
      OR UPPER(TABLE_NAME) LIKE '%${tableNameOnly.substring(0, 6)}%'
      ORDER BY OWNER, TABLE_NAME
    `;

    const result = await connections.oracle.execute(searchQuery);

    if (result.rows && result.rows.length > 0) {
      await logInfo(`   🔍 Found similar tables:`);
      result.rows.slice(0, 5).forEach((row: any) => {
        console.log(`      ${row[0]}.${row[1]}`);
      });
    } else {
      await logWarn(`   No similar tables found for ${tableNameOnly}`);
    }
  } catch (error) {
    // Ignore search errors
  }
}

/**
 * Test a simplified query with only accessible tables
 */
async function testSimplifiedQuery(
  connections: any,
  accessibleTables: string[],
) {
  await logInfo("\n🧪 Testing simplified query with accessible tables...");

  try {
    // Build a simple query with only core tables
    let simpleQuery = `
      SELECT 
        B.ID,
        A.FIRST_NAME,
        A.LAST_NAME,
        B.CREATED_DATE
      FROM CRVS.BIRTH_REGISTRATION B
      INNER JOIN CRVS.PERSON A ON B.CHILD_ID = A.ID
    `;

    // Add optional JOINs only for accessible tables
    if (accessibleTables.includes("CRVS.SEX")) {
      simpleQuery += ` LEFT JOIN CRVS.SEX H ON A.SEX_ID = H.ID`;
    }

    simpleQuery += ` WHERE ROWNUM <= 3`;

    await logInfo("Testing simplified query:");
    console.log("----------------------------------------");
    console.log(simpleQuery);
    console.log("----------------------------------------");

    const result = await connections.oracle.execute(simpleQuery, [], {
      outFormat: connections.oracle.constructor.OUT_FORMAT_OBJECT,
    });

    if (result.rows && result.rows.length > 0) {
      await logInfo(
        `✅ Simplified query works! Found ${result.rows.length} sample records`,
      );

      // Show sample data
      result.rows.forEach((row: any, index: number) => {
        console.log(
          `   Record ${index + 1}: ${row.FIRST_NAME} ${row.LAST_NAME} (ID: ${row.ID})`,
        );
      });

      return simpleQuery;
    } else {
      await logWarn("⚠️ Simplified query returned no data");
    }
  } catch (error) {
    await logError(`❌ Simplified query failed: ${error}`);
  }
}

// Export for use in other files
export { testOracleTableAccess };

// Run if called directly
if (require.main === module) {
  testOracleTableAccess().catch((error) => {
    console.error("❌ Table access test fatal error:", error);
    process.exit(1);
  });
}
