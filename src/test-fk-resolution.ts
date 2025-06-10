#!/usr/bin/env node

import { createConfig } from "./config/index.js";
import { createConfigFromEnv } from "./config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "./database/connections.js";
import {
  buildMappingCache,
  createResolvers,
} from "./utils/foreign-key-resolver.js";
import { COMMON_RESOLVERS } from "./utils/foreign-key-resolvers.js";
import { logInfo, logError } from "./utils/logger.js";

const testForeignKeyResolution = async (): Promise<void> => {
  console.log("🧪 Testing Foreign Key Resolution");
  console.log("===============================");
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    const config = createConfig(configOptions);
    connections = await initializeConnections(config);

    // Step 1: Check if region table has data
    await logInfo("🔍 Checking region table...");
    const client = await connections.postgresPool.connect();

    try {
      const regionCheck = await client.query(`
        SELECT COUNT(*) as count, 
               array_agg(code ORDER BY id LIMIT 5) as sample_codes,
               array_agg(id ORDER BY id LIMIT 5) as sample_ids
        FROM crvs_global.tbl_delimitation_region 
        WHERE code IS NOT NULL
      `);

      const regionCount = parseInt(regionCheck.rows[0].count);
      await logInfo(`📊 Found ${regionCount} regions in database`);

      if (regionCount > 0) {
        const sampleCodes = regionCheck.rows[0].sample_codes;
        const sampleIds = regionCheck.rows[0].sample_ids;
        await logInfo(`📋 Sample region codes: ${sampleCodes.join(", ")}`);
        await logInfo(`📋 Sample region IDs: ${sampleIds.join(", ")}`);
      } else {
        await logError(
          "❌ No regions found! Cannot test foreign key resolution.",
        );
        return;
      }

      // Step 2: Build cache manually
      await logInfo("🏗️ Building region cache...");
      await buildMappingCache(
        connections.postgresPool,
        "crvs_global.tbl_delimitation_region",
        "code",
        "id",
      );

      // Step 3: Create resolvers
      await logInfo("🔧 Creating resolvers...");
      const resolvers = createResolvers([COMMON_RESOLVERS.region]);

      // Step 4: Test resolution
      await logInfo("🧪 Testing region code resolution...");

      if (regionCount > 0) {
        const testCodes = regionCheck.rows[0].sample_codes;

        for (const code of testCodes) {
          if (code && resolvers.resolveRegionId) {
            try {
              const resolvedId = await resolvers.resolveRegionId(code);
              if (resolvedId) {
                await logInfo(
                  `✅ Successfully resolved: '${code}' -> ${resolvedId}`,
                );
              } else {
                await logError(`❌ Failed to resolve: '${code}' -> null`);
              }
            } catch (error) {
              await logError(`❌ Error resolving '${code}': ${error}`);
            }
          }
        }
      }

      await logInfo("🧪 Testing with common region codes...");
      const commonCodes = [
        "01",
        "02",
        "03",
        "1",
        "2",
        "3",
        "DSM",
        "DAR",
        "ARUSHA",
      ];

      for (const code of commonCodes) {
        if (resolvers.resolveRegionId) {
          try {
            const resolvedId = await resolvers.resolveRegionId(code);
            if (resolvedId) {
              await logInfo(
                `✅ Common code resolved: '${code}' -> ${resolvedId}`,
              );
            }
          } catch (error) {
            await logError(`❌ Error with common code '${code}': ${error}`);
          }
        }
      }
    } finally {
      client.release();
    }

    await logInfo("✅ Foreign key resolution test completed");
  } catch (error) {
    await logError(`❌ Test failed: ${error}`);
    throw error;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

if (require.main === module) {
  testForeignKeyResolution().catch((error) => {
    console.error("❌ Test fatal error:", error);
    process.exit(1);
  });
}
