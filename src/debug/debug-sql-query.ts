#!/usr/bin/env node

import { createConfig } from "../config/index.js";
import { createConfigFromEnv } from "../config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "../database/connections.js";
import { buildPaginatedQuery } from "../database/pagination.js";
import { loadQueryWithEnv } from "../utils/query-loader.js";
import { logInfo, logError } from "../utils/logger.js";

/**
 * Debug tool to see the exact SQL being generated
 */
const debugSqlQuery = async (): Promise<void> => {
  console.log("🔍 Debug SQL Query Generation");
  console.log("============================");
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    const config = createConfig(configOptions);
    connections = await initializeConnections(config);

    // Load the base query
    const baseQuery = loadQueryWithEnv("birth-registration.sql");

    await logInfo("📋 Original base query:");
    console.log("----------------------------------------");
    console.log(baseQuery);
    console.log("----------------------------------------");

    // Test different pagination strategies
    const paginationTests = [
      {
        name: "ROWNUM (offset 0, batch 1000)",
        context: {
          strategy: "rownum" as const,
          offset: 0,
          batchSize: 1000,
        },
      },
      {
        name: "ROWNUM (offset 1000, batch 1000)",
        context: {
          strategy: "rownum" as const,
          offset: 1000,
          batchSize: 1000,
        },
      },
      {
        name: "OFFSET (offset 0, batch 1000)",
        context: {
          strategy: "offset" as const,
          offset: 0,
          batchSize: 1000,
          orderByClause: "ORDER BY B.ID ASC",
        },
      },
    ];

    for (const test of paginationTests) {
      try {
        await logInfo(`\n🧪 Testing: ${test.name}`);
        const paginatedQuery = buildPaginatedQuery(baseQuery, test.context);

        console.log("Generated SQL:");
        console.log("----------------------------------------");
        console.log(paginatedQuery);
        console.log("----------------------------------------");

        // Test if Oracle can parse this query
        await logInfo("🔍 Testing Oracle syntax...");

        // Use EXPLAIN PLAN to test syntax without executing
        const explainQuery = `EXPLAIN PLAN FOR ${paginatedQuery}`;

        try {
          await connections.oracle.execute(explainQuery);
          await logInfo("✅ SQL syntax is valid");
        } catch (parseError: any) {
          await logError(`❌ SQL syntax error: ${parseError}`);

          // Try to identify the specific issue
          if (parseError.message.includes("ORA-00900")) {
            await logError("   This is an invalid SQL statement error");
          } else if (parseError.message.includes("ORA-00933")) {
            await logError("   This is a SQL command not properly ended error");
          } else if (parseError.message.includes("ORA-00936")) {
            await logError("   This is a missing expression error");
          }
        }
      } catch (error) {
        await logError(`❌ Error testing ${test.name}: ${error}`);
      }
    }

    await logInfo("\n🧪 Testing base query without pagination...");
    try {
      // Test with ROWNUM = 1 to get just one row
      const simpleTestQuery = `SELECT * FROM (${baseQuery}) WHERE ROWNUM = 1`;

      console.log("Simple test query:");
      console.log("----------------------------------------");
      console.log(simpleTestQuery);
      console.log("----------------------------------------");

      const result = await connections.oracle.execute(simpleTestQuery);
      await logInfo(
        `✅ Base query works - returned ${result.rows?.length || 0} rows`,
      );

      if (result.rows && result.rows.length > 0) {
        const sampleRow = result.rows[0];
        await logInfo("📋 Sample columns returned:");
        if (Array.isArray(sampleRow)) {
          await logInfo(`   Array with ${sampleRow.length} elements`);
        } else {
          await logInfo(
            `   Object with keys: ${Object.keys(sampleRow).join(", ")}`,
          );
        }
      }
    } catch (baseError) {
      await logError(`❌ Base query has issues: ${baseError}`);
    }
  } catch (error) {
    await logError(`❌ Debug failed: ${error}`);
    throw error;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

if (require.main === module) {
  debugSqlQuery().catch((error) => {
    console.error("❌ Debug fatal error:", error);
    process.exit(1);
  });
}

export { debugSqlQuery };
