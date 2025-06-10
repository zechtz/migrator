#!/usr/bin/env node

import { createConfig } from "./config/index.js";
import { createConfigFromEnv, validateConfiguration } from "./config/env.js";
import { runMigration } from "./migration/runner.js";
import { type MigrationTask } from "./types/index.js";
import { loadQueryWithEnv } from "./utils/query-loader.js";
import {
  regionTransformer,
  districtTransformer,
} from "./transformers/location-transformer.js";

const main = async (): Promise<void> => {
  console.log("🚀 Oracle to PostgreSQL Migration Tool (FIXED)");
  console.log("==============================================");
  console.log("");

  try {
    const configOptions = createConfigFromEnv();
    validateConfiguration(configOptions);

    // Fix checkpoint file path
    if (
      configOptions.checkpointFile &&
      configOptions.checkpointFile.endsWith("/")
    ) {
      configOptions.checkpointFile =
        configOptions.checkpointFile + "migration_checkpoint.json";
    }

    const config = createConfig(configOptions);

    // FIXED: Sequential migration by priority with proper dependencies
    const migrations: MigrationTask[] = [
      // Priority 1: Regions (must complete first)
      {
        sourceQuery: loadQueryWithEnv("regions.sql"),
        targetTable: "crvs_global.tbl_delimitation_region",
        transformFn: regionTransformer,
        priority: 1, // FIRST
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Priority 2: Districts (must wait for regions to complete)
      {
        sourceQuery: loadQueryWithEnv("districts.sql"),
        targetTable: "crvs_global.tbl_delimitation_district",
        transformFn: districtTransformer,
        priority: 2, // SECOND (after regions)
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },
    ];

    console.log(
      `📋 Starting FIXED migration of ${migrations.length} tables...`,
    );
    console.log("🔄 Processing by priority groups (sequential dependencies)");
    console.log("🔗 Foreign key resolution will work properly");
    console.log("");

    await runMigration(config, migrations);

    console.log("");
    console.log("✅ FIXED migration completed successfully!");
    console.log("");
    process.exit(0);
  } catch (error) {
    console.error("");
    console.error("❌ FIXED migration failed:", (error as Error).message);
    console.error("");
    process.exit(1);
  }
};

// Handle graceful shutdown
process.on("SIGINT", () => {
  console.log("\n🛑 FIXED migration interrupted");
  process.exit(0);
});

if (require.main === module) {
  main().catch((error) => {
    console.error("❌ FIXED migration fatal error:", error);
    process.exit(1);
  });
}
