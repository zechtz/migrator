#!/usr/bin/env node

import { createConfig } from "./config/index.js";
import { createConfigFromEnv, validateConfiguration } from "./config/env.js";
import { runMigration } from "./migration/runner.js";
import { type MigrationTask, type CacheDependency } from "./types/index.js";
import { loadQueryWithEnv } from "./utils/query-loader.js";
import { birthRegistrationTransformer } from "./transformers/birth-registration-transformer.js";
import {
  councilTransformer,
  districtTransformer,
  regionTransformer,
  registrationCenterTransformer,
  wardTransformer,
} from "./transformers/location-transformer.js";

const main = async (): Promise<void> => {
  console.log("🚀 Oracle to PostgreSQL Migration Tool");
  console.log("=====================================");
  console.log("");

  try {
    const configOptions = createConfigFromEnv();
    validateConfiguration(configOptions);
    const config = createConfig(configOptions);

    // Define migration tasks - the system automatically handles FK resolution
    const migrations: MigrationTask[] = [
      // Level 1: Regions (no foreign keys)
      {
        sourceQuery: loadQueryWithEnv("regions.sql"),
        targetTable: "crvs_global.tbl_delimitation_region",
        transformFn: regionTransformer, // Simple transformer
        priority: 1,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Level 2: Districts (depends on regions)
      {
        sourceQuery: loadQueryWithEnv("districts.sql"), // Includes REGION_CODE
        targetTable: "crvs_global.tbl_delimitation_district",
        transformFn: districtTransformer, // ✅ Uses resolveRegionId callback
        priority: 2,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Level 3: Councils (depends on districts)
      {
        sourceQuery: loadQueryWithEnv("councils.sql"), // Includes DISTRICT_CODE
        targetTable: "crvs_global.tbl_delimitation_council",
        transformFn: councilTransformer, // ✅ Uses resolveDistrictId callback
        priority: 3,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Level 4: Wards (depends on councils and districts)
      {
        sourceQuery: loadQueryWithEnv("wards.sql"), // Includes COUNCIL_CODE, DISTRICT_CODE
        targetTable: "crvs_global.tbl_delimitation_ward",
        transformFn: wardTransformer, // ✅ Uses multiple resolvers
        priority: 4,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Level 5: Registration Centers (depends on everything)
      {
        sourceQuery: loadQueryWithEnv("registration-centers.sql"), // Includes all parent codes
        targetTable: "crvs_global.tbl_mgt_registration_center",
        transformFn: registrationCenterTransformer, // ✅ Uses all resolvers
        priority: 5,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Your existing birth registration migration (unchanged)
      {
        sourceQuery: loadQueryWithEnv("birth-registration.sql"),
        targetTable: "registry.tbl_birth_certificate_info",
        transformFn: birthRegistrationTransformer, // Legacy transformer works fine
        priority: 10,
        paginationStrategy: "cursor",
        cursorColumn: "B.ID",
        orderByClause: "ORDER BY B.ID ASC",
        maxConcurrentBatches: 2,
      },
    ];

    console.log(`📋 Starting migration of ${migrations.length} tables...`);
    console.log(
      "🔗 Foreign key relationships will be resolved automatically via callbacks",
    );
    console.log("");

    await runMigration(config, migrations);

    console.log("");
    console.log("✅ Migration completed successfully!");
    console.log("");
    process.exit(0);
  } catch (error) {
    console.error("");
    console.error("❌ Migration failed:", (error as Error).message);
    console.error("");
    process.exit(1);
  }
};

// Handle graceful shutdown
process.on("SIGINT", () => {
  console.log("\n🛑 Received SIGINT. Shutting down gracefully...");
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("\n🛑 Received SIGTERM. Shutting down gracefully...");
  process.exit(0);
});

// Handle unhandled errors
process.on("unhandledRejection", (reason, promise) => {
  console.error("❌ Unhandled Rejection at:", promise, "reason:", reason);
  process.exit(1);
});

process.on("uncaughtException", (error) => {
  console.error("❌ Uncaught Exception:", error);
  process.exit(1);
});

// ✅ Add this (works in CommonJS):
if (require.main === module) {
  main().catch((error) => {
    console.error("❌ Fatal error:", error);
    process.exit(1);
  });
} // Run the migration if this file is executed directly
