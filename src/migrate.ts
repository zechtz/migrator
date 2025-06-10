#!/usr/bin/env node

import { createConfig } from "./config/index.js";
import { createConfigFromEnv, validateConfiguration } from "./config/env.js";
import { runMigration } from "./migration/runner.js";
import { type MigrationTask } from "./types/index.js";
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
  console.log(
    "🚀 Oracle to PostgreSQL Migration Tool (Enhanced FK Resolution)",
  );
  console.log(
    "==================================================================",
  );
  console.log("");

  try {
    const configOptions = createConfigFromEnv();
    validateConfiguration(configOptions);
    const config = createConfig(configOptions);

    const migrations: MigrationTask[] = [
      // Level 1: Regions (no foreign keys)
      {
        sourceQuery: loadQueryWithEnv("regions.sql"),
        targetTable: "crvs_global.tbl_delimitation_region",
        transformFn: regionTransformer,
        priority: 1,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Level 2: Districts (depends on regions)
      {
        sourceQuery: loadQueryWithEnv("districts.sql"),
        targetTable: "crvs_global.tbl_delimitation_district",
        transformFn: districtTransformer,
        priority: 2,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Level 3: Councils (depends on districts)
      {
        sourceQuery: loadQueryWithEnv("councils.sql"),
        targetTable: "crvs_global.tbl_delimitation_council",
        transformFn: councilTransformer,
        priority: 3,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Level 4: Wards (depends on councils and districts)
      {
        sourceQuery: loadQueryWithEnv("wards.sql"),
        targetTable: "crvs_global.tbl_delimitation_ward",
        transformFn: wardTransformer,
        priority: 4,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Level 5: Registration Centers (depends on everything)
      {
        sourceQuery: loadQueryWithEnv("registration-centers.sql"),
        targetTable: "crvs_global.tbl_mgt_registration_center",
        transformFn: registrationCenterTransformer, // ✅ Your existing enhanced transformer
        priority: 5,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Your existing birth registration migration (unchanged)
      {
        sourceQuery: loadQueryWithEnv("birth-registration.sql"),
        targetTable: "registry.tbl_birth_certificate_info",
        transformFn: birthRegistrationTransformer, // ✅ Your existing transformer
        priority: 10,
        paginationStrategy: "cursor",
        cursorColumn: "B.ID",
        orderByClause: "ORDER BY B.ID ASC",
        maxConcurrentBatches: 2,
      },
    ];

    console.log(`📋 Starting migration of ${migrations.length} tables...`);
    console.log("🔗 Enhanced foreign key resolution is now active");
    console.log("⚡ Caches will be built automatically at the right time");
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

process.on("SIGINT", () => {
  console.log("\n🛑 Received SIGINT. Shutting down gracefully...");
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("\n🛑 Received SIGTERM. Shutting down gracefully...");
  process.exit(0);
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("❌ Unhandled Rejection at:", promise, "reason:", reason);
  process.exit(1);
});

process.on("uncaughtException", (error) => {
  console.error("❌ Uncaught Exception:", error);
  process.exit(1);
});

if (require.main === module) {
  main().catch((error) => {
    console.error("❌ Fatal error:", error);
    process.exit(1);
  });
}
