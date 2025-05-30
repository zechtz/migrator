#!/usr/bin/env node

import { createConfig } from "./config/index.js";
import { createConfigFromEnv, validateConfiguration } from "./config/env.js";
import { runMigration } from "./migration/runner.js";
import { type MigrationTask } from "./types/index.js";
import { loadQueryWithEnv } from "./utils/query-loader.js";
import { birthRegistrationTransformer } from "./data/birth-registration-transformer.js";

/**
 * Main migration script that uses environment variables for configuration
 */
const main = async (): Promise<void> => {
  console.log("🚀 Oracle to PostgreSQL Migration Tool");
  console.log("=====================================");
  console.log("");

  try {
    // Load configuration from environment variables
    const configOptions = createConfigFromEnv();
    validateConfiguration(configOptions);

    const config = createConfig(configOptions);

    // Define your migration tasks here
    const migrations: MigrationTask[] = [
      {
        // 🔍 Query loaded from SQL file
        sourceQuery: loadQueryWithEnv("birth-registration.sql"),
        targetTable: "registry.tbl_birth_certificate_info",
        transformFn: birthRegistrationTransformer, // Use custom transformer
        priority: 10,
        paginationStrategy: "cursor",
        cursorColumn: "B.ID",
        orderByClause: "ORDER BY B.ID ASC",
        maxConcurrentBatches: 2,
      },
    ];

    console.log(`📋 Starting migration of ${migrations.length} tables...`);
    console.log("");

    // Run the migration
    await runMigration(config, migrations);

    console.log("");
    console.log("✅ Migration completed successfully!");
    console.log("");

    process.exit(0);
  } catch (error) {
    console.error("");
    console.error("❌ Migration failed:", (error as Error).message);
    console.error("");

    // Show helpful error information
    if ((error as Error).message.includes("Required environment variable")) {
      console.error(
        "💡 Make sure you have created a .env file with all required variables.",
      );
      console.error(
        "   Copy .env.example to .env and update with your database credentials.",
      );
    }

    if ((error as Error).message.includes("connect")) {
      console.error(
        "💡 Check your database connection settings and ensure the databases are running.",
      );
    }

    if ((error as Error).message.includes("Failed to load query")) {
      console.error(
        "💡 Make sure your SQL query files exist in the src/queries/ directory.",
      );
      console.error("   Expected file: src/queries/birth-registration.sql");
    }

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

// Run the migration if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error("❌ Fatal error:", error);
    process.exit(1);
  });
}
