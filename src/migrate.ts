#!/usr/bin/env node

import { createConfig } from "./config/index.js";
import { createConfigFromEnv, validateConfiguration } from "./config/env.js";
import {
  initializeConnections,
  closeConnections,
} from "./database/connections.js";
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
import { countryTransformer } from "./transformers/country-transformer.js";
import { clearCheckpoint } from "./data/checkpoint.js";
import { logInfo, logError } from "./utils/logger.js";
import { birthRegistrationTransformer } from "./transformers/birth-registration-transformer.js";
import {
  migrateTableEnhanced,
  MigrationResumeStrategy,
} from "./migration/enhanced-migration.js";

/**
 * Enhanced main migration with resume capabilities for all tables
 */
const main = async (): Promise<void> => {
  console.log("🚀 Enhanced Oracle to PostgreSQL Migration Tool");
  console.log("===============================================");
  console.log("");

  // Get resume strategy from command line
  const resumeStrategy = getResumeStrategy();
  console.log(`📋 Resume Strategy: ${resumeStrategy}`);
  console.log(`📋 This will: ${getStrategyDescription(resumeStrategy)}`);
  console.log("");

  let connections = null;

  try {
    const configOptions = createConfigFromEnv();
    validateConfiguration(configOptions);

    // Fix checkpoint file path if needed
    if (configOptions.checkpointFile?.endsWith("/")) {
      configOptions.checkpointFile += "migration_checkpoint.json";
    }

    const config = createConfig(configOptions);

    // Handle fresh start
    if (resumeStrategy === "fresh-start") {
      await clearCheckpoint(config.checkpointFile);
      console.log("🗑️ Cleared checkpoint for fresh start");
    }

    connections = await initializeConnections(config);

    // Define all migrations with enhanced configuration
    const migrations: EnhancedMigrationTask[] = [
      // Priority 1: Countries - lookup table
      {
        sourceQuery: loadQueryWithEnv("countries.sql"),
        targetTable: "crvs_global.tbl_delimitation_country",
        transformFn: countryTransformer,
        tableId: "countries",
        priority: 1,
        uniqueColumns: ["country_code"],
        resumeStrategy,
        description: "Country reference data",
      },

      // Priority 2: Registration center types - lookup table
      {
        sourceQuery: loadQueryWithEnv("registration-center-types.sql"),
        targetTable: "crvs_global.tbl_mgt_registration_center_type",
        transformFn: registrationCenterTypeTransformer,
        tableId: "registration_center_types",
        priority: 2,
        uniqueColumns: ["code"],
        resumeStrategy,
        description: "Registration center type reference data",
      },

      // Priority 3: Regions (no dependencies)
      {
        sourceQuery: loadQueryWithEnv("regions.sql"),
        targetTable: "crvs_global.tbl_delimitation_region",
        transformFn: regionTransformer,
        tableId: "regions",
        priority: 3,
        uniqueColumns: ["code"],
        resumeStrategy,
        description: "Region delimitation data",
      },

      // Priority 4: Districts (depends on regions)
      {
        sourceQuery: loadQueryWithEnv("districts.sql"),
        targetTable: "crvs_global.tbl_delimitation_district",
        transformFn: districtTransformer,
        tableId: "districts",
        priority: 4,
        uniqueColumns: ["code"],
        resumeStrategy,
        description: "District delimitation data",
        dependencies: ["regions"],
      },

      // Priority 5: Councils (depends on districts)
      {
        sourceQuery: loadQueryWithEnv("councils.sql"),
        targetTable: "crvs_global.tbl_delimitation_council",
        transformFn: councilTransformer,
        tableId: "councils",
        priority: 5,
        uniqueColumns: ["code"],
        resumeStrategy,
        description: "Council delimitation data",
        dependencies: ["districts"],
      },

      // Priority 6: Wards (depends on councils)
      {
        sourceQuery: loadQueryWithEnv("wards.sql"),
        targetTable: "crvs_global.tbl_delimitation_ward",
        transformFn: wardTransformer,
        tableId: "wards",
        priority: 6,
        uniqueColumns: ["code"],
        resumeStrategy,
        description: "Ward delimitation data",
        dependencies: ["councils"],
      },

      // Priority 7: Health facilities (depends on councils)
      {
        sourceQuery: loadQueryWithEnv("health-facilities.sql"),
        targetTable: "crvs_global.tbl_mgt_health_facility",
        transformFn: healthFacilityTransformer,
        tableId: "health_facilities",
        priority: 7,
        uniqueColumns: ["code"],
        resumeStrategy,
        description: "Health facility management data",
        dependencies: ["councils"],
      },

      // Priority 8: Registration Centers (depends on everything)
      {
        sourceQuery: loadQueryWithEnv("registration-centers.sql"),
        targetTable: "crvs_global.tbl_mgt_registration_center",
        transformFn: registrationCenterTransformer,
        tableId: "registration_centers",
        priority: 8,
        uniqueColumns: ["code"],
        resumeStrategy,
        description: "Registration center management data",
        dependencies: [
          "regions",
          "districts",
          "councils",
          "wards",
          "health_facilities",
        ],
      },

      // Priority 10: Birth registration (depends on all reference data)
      {
        sourceQuery: loadQueryWithEnv("birth-registration.sql"),
        targetTable: "registry.tbl_birth_certificate_info",
        transformFn: birthRegistrationTransformer,
        tableId: "birth_registration",
        priority: 10,
        uniqueColumns: ["provided_pin_no"],
        resumeStrategy,
        description: "Birth certificate registration data",
        dependencies: [
          "countries",
          "regions",
          "districts",
          "wards",
          "health_facilities",
        ],
        batchSize: 500, // Smaller batches for complex data
      },
    ];

    console.log(
      `📋 Starting enhanced migration of ${migrations.length} tables...`,
    );
    console.log("🔄 Processing by priority groups with resume capabilities");
    console.log(
      "🔗 Each table will check for existing data and handle duplicates",
    );
    console.log("");

    // Run enhanced migration by priority groups
    await runEnhancedMigrationByPriority(connections, config, migrations);

    console.log("");
    console.log("✅ Enhanced migration completed successfully!");
    console.log("");
    console.log("📊 Verification commands:");
    console.log("   npm run migrate:status    # Check migration status");
    console.log("   npm run migrate:inspect   # Inspect data quality");
    console.log("");
  } catch (error) {
    console.error("");
    console.error("❌ Enhanced migration failed:", (error as Error).message);
    console.error("");

    if ((error as Error).message.includes("ORA-")) {
      console.error(
        "💡 Oracle error detected. Check your Oracle connection and queries.",
      );
    }

    process.exit(1);
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
};

/**
 * Enhanced migration task interface
 */
interface EnhancedMigrationTask {
  sourceQuery: string;
  targetTable: string;
  transformFn: any;
  tableId: string;
  priority: number;
  uniqueColumns: string[];
  resumeStrategy: MigrationResumeStrategy;
  description: string;
  dependencies?: string[];
  batchSize?: number;
}

/**
 * Run enhanced migration by priority groups
 */
async function runEnhancedMigrationByPriority(
  connections: any,
  config: any,
  migrations: EnhancedMigrationTask[],
): Promise<void> {
  // Group by priority
  const priorityGroups = new Map<number, EnhancedMigrationTask[]>();

  migrations.forEach((migration) => {
    if (!priorityGroups.has(migration.priority)) {
      priorityGroups.set(migration.priority, []);
    }
    priorityGroups.get(migration.priority)!.push(migration);
  });

  // Process each priority group sequentially
  const sortedPriorities = Array.from(priorityGroups.keys()).sort(
    (a, b) => a - b,
  );

  for (const priority of sortedPriorities) {
    const groupMigrations = priorityGroups.get(priority)!;

    await logInfo(
      `📋 Processing priority group ${priority} (${groupMigrations.length} tables)`,
    );

    // Process all tables in this priority group concurrently
    const migrationPromises = groupMigrations.map((migration) =>
      runSingleEnhancedMigration(connections, config, migration),
    );

    const results = await Promise.allSettled(migrationPromises);

    const failures = results.filter((result) => result.status === "rejected");
    if (failures.length > 0) {
      await logError(
        `❌ ${failures.length} migrations failed in priority group ${priority}`,
      );
      failures.forEach((failure, index) => {
        if (failure.status === "rejected") {
          console.error(
            `   ${groupMigrations[index].tableId}: ${failure.reason}`,
          );
        }
      });
      throw new Error(
        `Migration failed for ${failures.length} tables in priority group ${priority}`,
      );
    }

    await logInfo(`✅ Completed priority group ${priority}`);
  }
}

/**
 * Run a single enhanced migration
 */
async function runSingleEnhancedMigration(
  connections: any,
  config: any,
  migration: EnhancedMigrationTask,
): Promise<number> {
  await logInfo(`🚀 Starting ${migration.description} (${migration.tableId})`);

  const migrationOptions = {
    resumeStrategy: migration.resumeStrategy,
    uniqueColumns: migration.uniqueColumns,
    batchSize: migration.batchSize || config.batchSize,
    checkExistingRecords: true,
  };

  try {
    const totalProcessed = await migrateTableEnhanced(
      connections,
      config,
      migration.sourceQuery,
      migration.targetTable,
      migration.tableId,
      migration.transformFn,
      migrationOptions,
    );

    await logInfo(
      `✅ Completed ${migration.description}: ${totalProcessed} records`,
    );
    return totalProcessed;
  } catch (error) {
    await logError(`❌ Failed ${migration.description}: ${error}`);
    throw error;
  }
}

/**
 * Get resume strategy from command line arguments
 */
function getResumeStrategy(): MigrationResumeStrategy {
  const args = process.argv.slice(2);

  // Look for --resume-strategy or --strategy argument
  const strategyIndex = args.findIndex(
    (arg) => arg === "--resume-strategy" || arg === "--strategy",
  );

  if (strategyIndex !== -1 && args[strategyIndex + 1]) {
    const strategy = args[strategyIndex + 1] as MigrationResumeStrategy;
    const validStrategies: MigrationResumeStrategy[] = [
      "skip-existing",
      "upsert",
      "fresh-start",
      "append-only",
    ];

    if (validStrategies.includes(strategy)) {
      return strategy;
    } else {
      console.warn(`⚠️ Invalid strategy "${strategy}", using "skip-existing"`);
    }
  }

  // Look for specific strategy flags
  if (args.includes("--fresh-start") || args.includes("--fresh")) {
    return "fresh-start";
  }
  if (args.includes("--upsert")) {
    return "upsert";
  }
  if (args.includes("--append-only") || args.includes("--append")) {
    return "append-only";
  }

  // Default to skip-existing (safest option)
  return "skip-existing";
}

/**
 * Get description of what each strategy does
 */
function getStrategyDescription(strategy: MigrationResumeStrategy): string {
  switch (strategy) {
    case "skip-existing":
      return "Check for existing records and skip duplicates (safest, recommended)";
    case "upsert":
      return "Update existing records, insert new ones (slower but most thorough)";
    case "fresh-start":
      return "Clear all destination tables and start over (⚠️ deletes existing data)";
    case "append-only":
      return "Just append new records without checking duplicates (fastest but may create duplicates)";
    default:
      return "Unknown strategy";
  }
}

/**
 * Show usage information
 */
function showUsage() {
  console.log("Enhanced Migration Usage:");
  console.log("========================");
  console.log("");
  console.log("Resume Strategies:");
  console.log(
    "  --strategy skip-existing   Skip records that already exist (default)",
  );
  console.log("  --strategy upsert         Update existing, insert new");
  console.log("  --strategy fresh-start    Clear all tables and start over");
  console.log(
    "  --strategy append-only    Just append (may create duplicates)",
  );
  console.log("");
  console.log("Short flags:");
  console.log("  --fresh       Same as --strategy fresh-start");
  console.log("  --upsert      Same as --strategy upsert");
  console.log("  --append      Same as --strategy append-only");
  console.log("");
  console.log("Examples:");
  console.log(
    "  npm run migrate                          # Skip existing (default)",
  );
  console.log("  npm run migrate -- --fresh-start         # Clear and restart");
  console.log(
    "  npm run migrate -- --strategy upsert     # Update existing records",
  );
  console.log("");
  console.log("Management Commands:");
  console.log("  npm run migrate:status    # Check migration status");
  console.log("  npm run migrate:inspect   # Inspect data quality");
  console.log("  npm run migrate:clear     # Clear checkpoint");
  console.log("");
}

// Show usage if help requested
if (process.argv.includes("--help") || process.argv.includes("-h")) {
  showUsage();
  process.exit(0);
}

// Handle graceful shutdown
process.on("SIGINT", () => {
  console.log("\n🛑 Enhanced migration interrupted");
  process.exit(0);
});

if (require.main === module) {
  main().catch((error) => {
    console.error("❌ Enhanced migration fatal error:", error);
    process.exit(1);
  });
}
