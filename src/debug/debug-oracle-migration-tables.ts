#!/usr/bin/env node

import oracledb from "oracledb";
import { createConfig } from "../config/index.js";
import { createConfigFromEnv } from "../config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "../database/connections.js";
import { logInfo, logError } from "../utils/logger.js";

const debugOracleAccess = async (): Promise<void> => {
  console.log("🔍 Enhanced Oracle Database Debug");
  console.log("===============================");
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    const config = createConfig(configOptions);
    connections = await initializeConnections(config);

    await logInfo("👤 Getting current user information...");

    const userInfoQuery = `
      SELECT 
        USER as current_user,
        SYS_CONTEXT('USERENV', 'SESSION_USER') as session_user,
        SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') as current_schema,
        SYS_CONTEXT('USERENV', 'DB_NAME') as database_name
      FROM DUAL
    `;

    const userResult = await connections.oracle.execute(userInfoQuery, [], {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });

    if (userResult.rows && userResult.rows.length > 0) {
      const userInfo = userResult.rows[0];
      await logInfo(`   Current User: ${userInfo.CURRENT_USER}`);
      await logInfo(`   Session User: ${userInfo.SESSION_USER}`);
      await logInfo(`   Current Schema: ${userInfo.CURRENT_SCHEMA}`);
      await logInfo(`   Database: ${userInfo.DATABASE_NAME}`);
    }

    await logInfo("\n🔍 Testing query output format...");

    const testQuery = `SELECT 'TEST_VALUE' as TEST_COLUMN FROM DUAL`;
    const testResult = await connections.oracle.execute(testQuery, [], {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });

    await logInfo(
      `   Test result structure: ${JSON.stringify(testResult.rows[0], null, 2)}`,
    );

    await logInfo("\n📋 Checking tables accessible to DASHBOARD user...");

    // Try different approaches to get table information
    const queries = [
      {
        name: "USER_TABLES (owned by current user)",
        query: "SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME",
      },
      {
        name: "ALL_TABLES (all accessible tables)",
        query:
          "SELECT TABLE_NAME, OWNER FROM ALL_TABLES ORDER BY OWNER, TABLE_NAME",
      },
      {
        name: "CRVS tables specifically",
        query:
          "SELECT TABLE_NAME, OWNER FROM ALL_TABLES WHERE OWNER = 'CRVS' ORDER BY TABLE_NAME",
      },
      {
        name: "All schemas with tables",
        query: "SELECT DISTINCT OWNER FROM ALL_TABLES ORDER BY OWNER",
      },
    ];

    for (const queryInfo of queries) {
      try {
        await logInfo(`\n🔍 Testing: ${queryInfo.name}`);

        const result = await connections.oracle.execute(queryInfo.query, [], {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
        });

        if (result.rows && result.rows.length > 0) {
          await logInfo(`   ✅ Found ${result.rows.length} results`);

          // Show first few results with full structure
          const sampleSize = Math.min(5, result.rows.length);
          for (let i = 0; i < sampleSize; i++) {
            const row = result.rows[i];
            await logInfo(`   Row ${i + 1}: ${JSON.stringify(row)}`);
          }

          if (result.rows.length > sampleSize) {
            await logInfo(
              `   ... and ${result.rows.length - sampleSize} more rows`,
            );
          }
        } else {
          await logInfo(`   ❌ No results found`);
        }
      } catch (error) {
        await logError(`   ❌ Query failed: ${error}`);
      }
    }

    await logInfo("\n🎯 Searching for registration center type table...");

    const registrationQueries = [
      "SELECT TABLE_NAME, OWNER FROM ALL_TABLES WHERE UPPER(TABLE_NAME) LIKE '%REGISTRATION%CENTER%TYPE%'",
      "SELECT TABLE_NAME, OWNER FROM ALL_TABLES WHERE UPPER(TABLE_NAME) LIKE '%REG%CENTER%TYPE%'",
      "SELECT TABLE_NAME, OWNER FROM ALL_TABLES WHERE UPPER(TABLE_NAME) = 'REGISTRATION_CENTER_TYPE'",
      "SELECT TABLE_NAME, OWNER FROM ALL_TABLES WHERE UPPER(TABLE_NAME) LIKE '%REGISTRATION%'",
    ];

    for (const query of registrationQueries) {
      try {
        const result = await connections.oracle.execute(query, [], {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
        });

        if (result.rows && result.rows.length > 0) {
          await logInfo(`✅ Found ${result.rows.length} matching tables:`);
          result.rows.forEach((row: any) => {
            logInfo(`   ${row.OWNER}.${row.TABLE_NAME}`);
          });
        }
      } catch (error) {
        await logError(`Query failed: ${error}`);
      }
    }

    await logInfo("\n🧪 Testing direct table access...");

    const directQueries = [
      "SELECT COUNT(*) as row_count FROM CRVS.REGISTRATION_CENTER_TYPE",
      "SELECT * FROM CRVS.REGISTRATION_CENTER_TYPE WHERE ROWNUM <= 3",
      "DESC CRVS.REGISTRATION_CENTER_TYPE", // This might not work in all Oracle versions
    ];

    for (const query of directQueries) {
      try {
        await logInfo(`\n   Testing: ${query}`);
        const result = await connections.oracle.execute(query, [], {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
        });

        if (result.rows && result.rows.length > 0) {
          await logInfo(`   ✅ Success! Results:`);
          result.rows.forEach((row: any, index: number) => {
            logInfo(`     Row ${index + 1}: ${JSON.stringify(row)}`);
          });
        } else {
          await logInfo(`   ⚠️ Query executed but no rows returned`);
        }
      } catch (error) {
        await logError(`   ❌ Direct access failed: ${error}`);
      }
    }

    await logInfo("\n📊 Getting column information...");

    try {
      const columnsQuery = `
        SELECT 
          COLUMN_NAME, 
          DATA_TYPE, 
          DATA_LENGTH, 
          NULLABLE,
          DATA_DEFAULT
        FROM ALL_TAB_COLUMNS 
        WHERE OWNER = 'CRVS' 
        AND TABLE_NAME = 'REGISTRATION_CENTER_TYPE'
        ORDER BY COLUMN_ID
      `;

      const columnsResult = await connections.oracle.execute(columnsQuery, [], {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
      });

      if (columnsResult.rows && columnsResult.rows.length > 0) {
        await logInfo(`✅ Table structure for CRVS.REGISTRATION_CENTER_TYPE:`);
        columnsResult.rows.forEach((row: any) => {
          logInfo(
            `   ${row.COLUMN_NAME}: ${row.DATA_TYPE}(${row.DATA_LENGTH}) ${row.NULLABLE === "Y" ? "NULL" : "NOT NULL"}`,
          );
        });
      } else {
        await logError(`❌ Could not get column information`);
      }
    } catch (error) {
      await logError(`❌ Column query failed: ${error}`);
    }

    await logInfo("\n🔒 Checking user privileges...");

    try {
      const privQuery = `
        SELECT 
          GRANTEE,
          OWNER,
          TABLE_NAME,
          PRIVILEGE,
          GRANTABLE
        FROM ALL_TAB_PRIVS 
        WHERE GRANTEE = USER 
        AND OWNER = 'CRVS'
        AND TABLE_NAME = 'REGISTRATION_CENTER_TYPE'
      `;

      const privResult = await connections.oracle.execute(privQuery, [], {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
      });

      if (privResult.rows && privResult.rows.length > 0) {
        await logInfo(`✅ Privileges on CRVS.REGISTRATION_CENTER_TYPE:`);
        privResult.rows.forEach((row: any) => {
          logInfo(
            `   ${row.PRIVILEGE} (${row.GRANTABLE === "YES" ? "grantable" : "not grantable"})`,
          );
        });
      } else {
        await logInfo(
          `⚠️ No explicit privileges found (may have access through role)`,
        );
      }
    } catch (error) {
      await logError(`❌ Privilege check failed: ${error}`);
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
  debugOracleAccess().catch((error) => {
    console.error("❌ Debug fatal error:", error);
    process.exit(1);
  });
}
