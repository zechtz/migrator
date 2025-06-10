#!/usr/bin/env node

import { createConfig } from "../config/index.js";
import { createConfigFromEnv } from "../config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "../database/connections.js";
import { loadQueryWithEnv } from "../utils/query-loader.js";
import { fetchOracleDataWithPagination } from "../database/oracle.js";
import { logInfo, logError } from "../utils/logger.js";

/**
 * Check Oracle data fields to understand the structure
 */
const checkOracleFields = async (): Promise<void> => {
  console.log("🔍 Checking Oracle Data Fields");
  console.log("=============================");
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    const config = createConfig(configOptions);
    connections = await initializeConnections(config);

    // Check regions data
    await logInfo("🔍 Checking REGIONS data...");
    const regionsQuery = loadQueryWithEnv("regions.sql");
    const regionsResult = await fetchOracleDataWithPagination(
      connections.oracle,
      regionsQuery,
      { strategy: "rownum", offset: 0, batchSize: 5 },
    );

    if (regionsResult.rows.length > 0) {
      await logInfo(`📊 Found ${regionsResult.rows.length} region samples`);
      await logInfo(
        `📋 Region fields: ${Object.keys(regionsResult.rows[0]).join(", ")}`,
      );

      // Show sample data
      regionsResult.rows.forEach((row, index) => {
        logInfo(
          `   Region ${index + 1}: CODE='${row.CODE}', NAME='${row.NAME}', ID='${row.ID}'`,
        );
      });
    } else {
      await logError("❌ No regions found in Oracle");
    }

    // Check districts data
    await logInfo("\n🔍 Checking DISTRICTS data...");
    const districtsQuery = loadQueryWithEnv("districts.sql");
    const districtsResult = await fetchOracleDataWithPagination(
      connections.oracle,
      districtsQuery,
      { strategy: "rownum", offset: 0, batchSize: 5 },
    );

    if (districtsResult.rows.length > 0) {
      await logInfo(`📊 Found ${districtsResult.rows.length} district samples`);
      await logInfo(
        `📋 District fields: ${Object.keys(districtsResult.rows[0]).join(", ")}`,
      );

      // Show sample data and identify region field
      districtsResult.rows.forEach((row, index) => {
        const regionFields = Object.keys(row).filter((key) =>
          key.toLowerCase().includes("region"),
        );

        let regionInfo = "";
        regionFields.forEach((field) => {
          regionInfo += ` ${field}='${row[field]}'`;
        });

        logInfo(
          `   District ${index + 1}: CODE='${row.CODE}', NAME='${row.NAME}', ID='${row.ID}'${regionInfo}`,
        );
      });

      // Identify the region reference field
      const sampleRow = districtsResult.rows[0];
      const regionFields = Object.keys(sampleRow).filter((key) =>
        key.toLowerCase().includes("region"),
      );

      if (regionFields.length > 0) {
        await logInfo(
          `\n🎯 Found region reference fields: ${regionFields.join(", ")}`,
        );
        regionFields.forEach((field) => {
          logInfo(
            `   ${field} sample values: ${districtsResult.rows.map((r) => r[field]).join(", ")}`,
          );
        });
      } else {
        await logError("❌ No region reference fields found in districts!");
        await logInfo("📋 Available fields for region lookup:");
        Object.keys(sampleRow).forEach((field) => {
          logInfo(`   ${field}: '${sampleRow[field]}'`);
        });
      }
    } else {
      await logError("❌ No districts found in Oracle");
    }
  } catch (error) {
    await logError(`❌ Field check failed: ${error}`);
    throw error;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

if (require.main === module) {
  checkOracleFields().catch((error) => {
    console.error("❌ Field check fatal error:", error);
    process.exit(1);
  });
}
