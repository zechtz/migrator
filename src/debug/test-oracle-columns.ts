#!/usr/bin/env node

import { createConfig } from "../config/index.js";
import { createConfigFromEnv } from "../config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "../database/connections.js";
import { logInfo, logError } from "../utils/logger.js";

/**
 * Check the actual column names in Oracle tables
 */
const checkOracleColumns = async (): Promise<void> => {
  console.log("🔍 Checking Oracle Column Names");
  console.log("===============================");
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    const config = createConfig(configOptions);
    connections = await initializeConnections(config);

    // Key tables to check
    const tablesToCheck = [
      "CRVS.PERSON",
      "CRVS.BIRTH_REGISTRATION",
      "CRVS.SEX",
      "CRVS.REGION",
      "CRVS.DISTRICT",
    ];

    for (const table of tablesToCheck) {
      await checkTableColumns(connections, table);
    }

    // Test a simple query with correct column names
    await testSimpleQueries(connections);
  } catch (error) {
    await logError(`❌ Column check failed: ${error}`);
    throw error;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

/**
 * Check columns for a specific table
 */
async function checkTableColumns(connections: any, tableName: string) {
  try {
    await logInfo(`\n📋 Checking columns for ${tableName}:`);

    // Get column information from Oracle data dictionary
    const columnQuery = `
      SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
      FROM ALL_TAB_COLUMNS
      WHERE OWNER = '${tableName.split(".")[0]}'
      AND TABLE_NAME = '${tableName.split(".")[1]}'
      ORDER BY COLUMN_ID
    `;

    const result = await connections.oracle.execute(columnQuery);

    if (result.rows && result.rows.length > 0) {
      console.log("   Columns:");
      result.rows.forEach((row: any) => {
        const [colName, dataType, length, nullable] = row;
        console.log(
          `     ${colName} (${dataType}${length ? `(${length})` : ""}) ${nullable === "Y" ? "NULL" : "NOT NULL"}`,
        );
      });

      // Test a simple SELECT with the first few columns
      await testTableSample(connections, tableName, result.rows);
    } else {
      await logError(`   ❌ No columns found for ${tableName}`);
    }
  } catch (error) {
    await logError(`   ❌ Error checking ${tableName}: ${error}`);
  }
}

/**
 * Test a simple SELECT with actual column names
 */
async function testTableSample(
  connections: any,
  tableName: string,
  columns: any[],
) {
  try {
    // Get first 3-5 column names for testing
    const testColumns = columns
      .slice(0, Math.min(5, columns.length))
      .map((row: any) => row[0]) // Column name is first element
      .join(", ");

    const testQuery = `SELECT ${testColumns} FROM ${tableName} WHERE ROWNUM <= 2`;

    await logInfo(
      `   🧪 Testing query: SELECT ${testColumns} FROM ${tableName}...`,
    );

    const result = await connections.oracle.execute(testQuery, [], {
      outFormat: connections.oracle.constructor.OUT_FORMAT_OBJECT,
    });

    if (result.rows && result.rows.length > 0) {
      await logInfo(`   ✅ Sample data (${result.rows.length} rows):`);
      result.rows.forEach((row: any, index: number) => {
        console.log(`     Row ${index + 1}:`, JSON.stringify(row, null, 2));
      });
    } else {
      await logInfo(`   ⚠️ No data returned from ${tableName}`);
    }
  } catch (error) {
    await logError(`   ❌ Sample query failed for ${tableName}: ${error}`);
  }
}

/**
 * Test simple queries to build up to birth registration
 */
async function testSimpleQueries(connections: any) {
  await logInfo("\n🧪 Testing Progressive Queries:");
  await logInfo("===============================");

  const queries = [
    {
      name: "Basic PERSON table",
      query: "SELECT * FROM CRVS.PERSON WHERE ROWNUM = 1",
    },
    {
      name: "Basic BIRTH_REGISTRATION table",
      query: "SELECT * FROM CRVS.BIRTH_REGISTRATION WHERE ROWNUM = 1",
    },
    {
      name: "JOIN PERSON + BIRTH_REGISTRATION",
      query: `
        SELECT P.*, B.*
        FROM CRVS.PERSON P, CRVS.BIRTH_REGISTRATION B
        WHERE P.ID = B.CHILD_ID AND ROWNUM = 1
      `,
    },
  ];

  for (const { name, query } of queries) {
    try {
      await logInfo(`\n🔍 ${name}:`);
      console.log(`Query: ${query}`);

      const result = await connections.oracle.execute(query, [], {
        outFormat: connections.oracle.constructor.OUT_FORMAT_OBJECT,
      });

      if (result.rows && result.rows.length > 0) {
        const row = result.rows[0];
        await logInfo(
          `✅ Success! Returned ${Object.keys(row).length} columns:`,
        );

        // Show first 10 column names and values
        const columns = Object.keys(row).slice(0, 10);
        columns.forEach((col) => {
          console.log(`     ${col}: ${row[col]}`);
        });

        if (Object.keys(row).length > 10) {
          console.log(
            `     ... and ${Object.keys(row).length - 10} more columns`,
          );
        }
      } else {
        await logInfo(`⚠️ Query returned no data`);
      }
    } catch (error) {
      await logError(`❌ ${name} failed: ${error}`);
    }
  }
}

// Run if called directly
if (require.main === module) {
  checkOracleColumns().catch((error) => {
    console.error("❌ Column check fatal error:", error);
    process.exit(1);
  });
}

export { checkOracleColumns };
