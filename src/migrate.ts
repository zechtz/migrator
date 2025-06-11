#!/usr/bin/env node

import { createConfig } from "./config/index.js";
import { createConfigFromEnv, validateConfiguration } from "./config/env.js";
import { runMigration } from "./migration/runner.js";
import { type MigrationTask } from "./types/index.js";
import { loadQueryWithEnv } from "./utils/query-loader.js";
import {
  regionTransformer,
  districtTransformer,
  councilTransformer,
  wardTransformer,
  healthFacilityTransformer,
  registrationCenterTransformer,
  registrationCenterTypeTransformer,
} from "./transformers/location-transformer.js";
import { countryTransformer } from "transformers/country-transformer.js";
import { birthRegistrationTransformer } from "transformers/birth-registration-transformer.js";

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

    const migrations: MigrationTask[] = [
      // Priority 1: country - lookup table
      {
        sourceQuery: loadQueryWithEnv("countries.sql"),
        targetTable: "crvs_global.tbl_delimitation_country",
        transformFn: countryTransformer,
        priority: 1,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },
      // Priority 2: registration-center-types - lookup table
      {
        sourceQuery: loadQueryWithEnv("registration-center-types.sql"),
        targetTable: "crvs_global.tbl_mgt_registration_center_type",
        transformFn: registrationCenterTypeTransformer,
        priority: 2,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Priority 3: Regions (must complete first - no dependencies)
      {
        sourceQuery: loadQueryWithEnv("regions.sql"),
        targetTable: "crvs_global.tbl_delimitation_region",
        transformFn: regionTransformer,
        priority: 3,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Priority 4: Districts (depends on regions)
      {
        sourceQuery: loadQueryWithEnv("districts.sql"),
        targetTable: "crvs_global.tbl_delimitation_district",
        transformFn: districtTransformer,
        priority: 4,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Priority 5: Councils (depends on districts)
      {
        sourceQuery: loadQueryWithEnv("councils.sql"),
        targetTable: "crvs_global.tbl_delimitation_council",
        transformFn: councilTransformer,
        priority: 5,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Priority 6: Wards (depends on councils)
      {
        sourceQuery: loadQueryWithEnv("wards.sql"),
        targetTable: "crvs_global.tbl_delimitation_ward",
        transformFn: wardTransformer,
        priority: 6,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Priority 7: Health facilities (depends on councils)
      {
        sourceQuery: loadQueryWithEnv("health-facilities.sql"),
        targetTable: "crvs_global.tbl_mgt_health_facility",
        transformFn: healthFacilityTransformer,
        priority: 7,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // Level 8: Registration Centers (depends on everything)
      {
        sourceQuery: loadQueryWithEnv("registration-centers.sql"),
        targetTable: "crvs_global.tbl_mgt_registration_center",
        transformFn: registrationCenterTransformer,
        priority: 8,
        paginationStrategy: "rownum",
        maxConcurrentBatches: 1,
      },

      // birth registration migration last priority
      {
        sourceQuery: loadQueryWithEnv("birth-registration.sql"),
        targetTable: "registry.tbl_birth_certificate_info",
        transformFn: birthRegistrationTransformer,
        priority: 10,

        paginationStrategy: "rownum",
        maxConcurrentBatches: 2,
        // Use insert mode initially
        migrationMode: "insert",
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
