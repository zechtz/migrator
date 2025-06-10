#!/usr/bin/env node

import { createConfig } from "./config/index.js";
import { createConfigFromEnv, validateConfiguration } from "./config/env.js";
import { runMigration } from "./migration/runner.js";
import { type MigrationTask } from "./types/index.js";
import { loadQueryWithEnv } from "./utils/query-loader.js";
import { regionTransformer } from "./transformers/location-transformer.js";
import { debugDistrictTransformer } from "./transformers/debug-location-transformer.js";

const main = async (): Promise<void> => {
  console.log("🐛 DEBUG Oracle to PostgreSQL Migration Tool");
  console.log("===========================================");
  console.log("");

  try {
    const configOptions = createConfigFromEnv();
    validateConfiguration(configOptions);
    const config = createConfig(configOptions);

    // 🐛 Debug version - only migrate regions and districts for testing
    const migrations: MigrationTask[] = [
      // Regions first (reference table)
      {
        sourceQuery: loadQueryWithEnv("regions.sql"),
        targetTable: "crvs_global.tbl_delimitation_region",
        transformFn: regionTransformer,
        priority: 1,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Districts with debug transformer
      {
        sourceQuery: loadQueryWithEnv("districts.sql"),
        targetTable: "crvs_global.tbl_delimitation_district",
        transformFn: debugDistrictTransformer, // 🐛 Debug version with extensive logging
        priority: 2,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },
    ];

    console.log(
      `🐛 Starting DEBUG migration of ${migrations.length} tables...`,
    );
    console.log("📊 This will show detailed FK resolution logging");
    console.log("");

    // ✅ Same function call - enhanced runner handles everything
    await runMigration(config, migrations);

    console.log("");
    console.log("✅ Debug migration completed!");
    console.log("");
    process.exit(0);
  } catch (error) {
    console.error("");
    console.error("❌ Debug migration failed:", (error as Error).message);
    console.error("");
    process.exit(1);
  }
};

if (require.main === module) {
  main().catch((error) => {
    console.error("❌ Debug migration fatal error:", error);
    process.exit(1);
  });
}
