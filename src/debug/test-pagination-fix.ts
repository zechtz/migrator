#!/usr/bin/env node

import { createConfig } from "../config/index.js";
import { createConfigFromEnv } from "../config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "../database/connections.js";
import { buildPaginatedQuery } from "../database/pagination.js";
import { logInfo, logError } from "../utils/logger.js";

/**
 * Test if pagination works once we have correct column names
 */
const testPaginationFix = async (): Promise<void> => {
  console.log("🔍 Testing Pagination with Correct Column Names");
  console.log("===============================================");
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    const config = createConfig(configOptions);
    connections = await initializeConnections(config);

    // Test progressively complex queries with pagination
    const testQueries = [
      {
        name: "Simple PERSON query",
        baseQuery:
          "SELECT ID, FIRST_NAME, LAST_NAME FROM CRVS.PERSON ORDER BY ID",
      },
      {
        name: "PERSON + BIRTH_REGISTRATION",
        baseQuery: `
          SELECT 
            P.ID as PERSON_ID,
            P.FIRST_NAME,
            P.LAST_NAME,
            B.ID as BIRTH_REG_ID,
            B.CREATED_DATE
          FROM CRVS.PERSON P
          INNER JOIN CRVS.BIRTH_REGISTRATION B ON P.ID = B.CHILD_ID
          ORDER BY B.ID
        `,
      },
    ];

    for (const { name, baseQuery } of testQueries) {
      await testQueryWithPagination(connections, name, baseQuery);
    }
  } catch (error) {
    await logError(`❌ Pagination test failed: ${error}`);
    throw error;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

/**
 * Test a query with different pagination strategies
 */
async function testQueryWithPagination(
  connections: any,
  name: string,
  baseQuery: string,
) {
  await logInfo(`\n🧪 Testing: ${name}`);
  console.log("Base query:", baseQuery.replace(/\s+/g, " ").trim());

  const paginationTests = [
    {
      name: "ROWNUM (0-5)",
      context: { strategy: "rownum" as const, offset: 0, batchSize: 5 },
    },
    {
      name: "ROWNUM (5-10)",
      context: { strategy: "rownum" as const, offset: 5, batchSize: 5 },
    },
  ];

  for (const { name: testName, context } of paginationTests) {
    try {
      await logInfo(`   ${testName}:`);

      // Build paginated query
      const paginatedQuery = buildPaginatedQuery(baseQuery, context);

      // Test syntax with EXPLAIN PLAN
      const explainQuery = `EXPLAIN PLAN FOR ${paginatedQuery}`;
      await connections.oracle.execute(explainQuery);
      await logInfo(`     ✅ SQL syntax valid`);

      // Test actual execution
      const result = await connections.oracle.execute(paginatedQuery, [], {
        outFormat: connections.oracle.constructor.OUT_FORMAT_OBJECT,
      });

      if (result.rows && result.rows.length > 0) {
        await logInfo(`     ✅ Returned ${result.rows.length} rows`);

        // Show sample
        const sampleRow = result.rows[0];
        const columns = Object.keys(sampleRow).slice(0, 3);
        const sample = columns
          .map((col) => `${col}=${sampleRow[col]}`)
          .join(", ");
        console.log(`       Sample: ${sample}`);
      } else {
        await logInfo(`     ⚠️ No data returned`);
      }
    } catch (error: any) {
      await logError(
        `     ❌ ${testName} failed: ${error.message.split("\n")[0]}`,
      );
    }
  }
}

// Run if called directly
if (require.main === module) {
  testPaginationFix().catch((error) => {
    console.error("❌ Pagination test fatal error:", error);
    process.exit(1);
  });
}

export { testPaginationFix };
