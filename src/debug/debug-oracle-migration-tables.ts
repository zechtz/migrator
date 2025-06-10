#!/usr/bin/env node

import { createConfig } from "../config/index.js";
import { createConfigFromEnv } from "../config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "../database/connections.js";
import { logInfo, logError } from "../utils/logger.js";

const debugOracleTables = async (): Promise<void> => {
  console.log("🔍 Debug Oracle Tables - Registration Center Types");
  console.log("================================================");
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    const config = createConfig(configOptions);
    connections = await initializeConnections(config);

    // Check for registration center type tables
    await logInfo("🔍 Searching for registration center type tables...");

    const searchQuery = `
      SELECT TABLE_NAME, OWNER 
      FROM ALL_TABLES 
      WHERE UPPER(TABLE_NAME) LIKE '%REGISTRATION%'
         OR UPPER(TABLE_NAME) LIKE '%CENTER%'
         OR UPPER(TABLE_NAME) LIKE '%TYPE%'
      ORDER BY OWNER, TABLE_NAME
    `;

    const searchResult = await connections.oracle.execute(searchQuery, [], {
      outFormat: connections.oracle.constructor.OUT_FORMAT_OBJECT,
    });

    if (searchResult.rows && searchResult.rows.length > 0) {
      await logInfo(`📋 Found ${searchResult.rows.length} related tables:`);
      searchResult.rows.forEach((row: any) => {
        logInfo(`   ${row.OWNER}.${row.TABLE_NAME}`);
      });
    } else {
      await logError("❌ No related tables found!");
    }

    // Check specifically in CRVS schema
    await logInfo("\n🔍 Checking CRVS schema tables...");

    const crvsQuery = `
      SELECT TABLE_NAME 
      FROM ALL_TABLES 
      WHERE OWNER = 'CRVS' 
      ORDER BY TABLE_NAME
    `;

    const crvsResult = await connections.oracle.execute(crvsQuery, [], {
      outFormat: connections.oracle.constructor.OUT_FORMAT_OBJECT,
    });

    if (crvsResult.rows && crvsResult.rows.length > 0) {
      await logInfo(`📋 CRVS schema has ${crvsResult.rows.length} tables:`);
      crvsResult.rows.forEach((row: any, index: number) => {
        if (index < 20) {
          // Show first 20 tables
          logInfo(`   ${row.TABLE_NAME}`);
        }
      });
      if (crvsResult.rows.length > 20) {
        logInfo(`   ... and ${crvsResult.rows.length - 20} more tables`);
      }
    } else {
      await logError("❌ No tables found in CRVS schema!");
    }

    // Try to find the exact table we need
    await logInfo("\n🔍 Looking for exact registration center type table...");

    const exactQuery = `
      SELECT TABLE_NAME, OWNER, NUM_ROWS
      FROM ALL_TABLES 
      WHERE UPPER(TABLE_NAME) IN (
        'REGISTRATION_CENTER_TYPE',
        'REG_CENTER_TYPE', 
        'REGCENTER_TYPE',
        'REGISTRATION_CENTRE_TYPE'
      )
      ORDER BY OWNER, TABLE_NAME
    `;

    const exactResult = await connections.oracle.execute(exactQuery, [], {
      outFormat: connections.oracle.constructor.OUT_FORMAT_OBJECT,
    });

    if (exactResult.rows && exactResult.rows.length > 0) {
      await logInfo("✅ Found exact matches:");
      exactResult.rows.forEach((row: any) => {
        logInfo(
          `   ${row.OWNER}.${row.TABLE_NAME} (${row.NUM_ROWS || "unknown"} rows)`,
        );
      });
    } else {
      await logError(
        "❌ No exact matches found for registration center type table!",
      );
    }

    // Check current user permissions
    await logInfo("\n🔍 Checking current user and permissions...");

    const userQuery = `SELECT USER FROM DUAL`;
    const userResult = await connections.oracle.execute(userQuery);

    if (userResult.rows && userResult.rows.length > 0) {
      await logInfo(`👤 Current Oracle user: ${userResult.rows[0][0]}`);
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
  debugOracleTables().catch((error) => {
    console.error("❌ Debug fatal error:", error);
    process.exit(1);
  });
}
