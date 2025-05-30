#!/usr/bin/env node

import { createConfigFromEnv, validateConfiguration } from "./config/env.js";

/**
 * CLI helper for managing the migration tool
 */
const command = process.argv[2];

switch (command) {
  case "config":
  case "check-config":
    checkConfig();
    break;

  case "env":
  case "check-env":
    checkEnvironment();
    break;

  case "help":
  case "--help":
  case "-h":
    showHelp();
    break;

  default:
    if (command) {
      console.error(`❌ Unknown command: ${command}`);
      console.error("");
    }
    showHelp();
    process.exit(command ? 1 : 0);
}

/**
 * Check and display current configuration
 */
function checkConfig() {
  console.log("🔧 Checking Configuration...");
  console.log("");

  try {
    const config = createConfigFromEnv();
    validateConfiguration(config);
    console.log("✅ Configuration is valid!");
  } catch (error) {
    console.error("❌ Configuration Error:", (error as Error).message);
    process.exit(1);
  }
}

/**
 * Check environment variables
 */
function checkEnvironment() {
  console.log("🌍 Environment Variables Check");
  console.log("============================");
  console.log("");

  const requiredVars = [
    "ORACLE_HOST",
    "ORACLE_USER",
    "ORACLE_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_DATABASE",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
  ];

  const optionalVars = [
    "ORACLE_PORT",
    "ORACLE_SID",
    "ORACLE_SERVICE_NAME",
    "POSTGRES_PORT",
    "BATCH_SIZE",
    "MAX_RETRIES",
    "RETRY_DELAY",
    "MAX_CONCURRENT_BATCHES",
    "MAX_CONCURRENT_TABLES",
    "PAGINATION_STRATEGY",
    "CURSOR_COLUMN",
  ];

  let hasErrors = false;

  console.log("Required Variables:");
  requiredVars.forEach((varName) => {
    const value = process.env[varName];
    if (value) {
      console.log(`  ✅ ${varName}: ${"*".repeat(Math.min(value.length, 8))}`);
    } else {
      console.log(`  ❌ ${varName}: NOT SET`);
      hasErrors = true;
    }
  });

  // Special check for Oracle connection method
  const hasSid = process.env.ORACLE_SID;
  const hasServiceName = process.env.ORACLE_SERVICE_NAME;

  console.log("");
  console.log("Oracle Connection Method:");
  if (hasSid) {
    console.log(`  ✅ ORACLE_SID: ${hasSid}`);
  }
  if (hasServiceName) {
    console.log(`  ✅ ORACLE_SERVICE_NAME: ${hasServiceName}`);
  }

  if (!hasSid && !hasServiceName) {
    console.log(`  ❌ Either ORACLE_SID or ORACLE_SERVICE_NAME must be set`);
    hasErrors = true;
  }

  if (hasSid && hasServiceName) {
    console.log(`  ⚠️  Both SID and Service Name set - will use SID`);
  }

  console.log("");
  console.log("Optional Variables:");
  optionalVars.forEach((varName) => {
    const value = process.env[varName];
    if (value) {
      console.log(`  ✅ ${varName}: ${value}`);
    } else {
      console.log(`  ⚪ ${varName}: using default`);
    }
  });

  console.log("");

  if (hasErrors) {
    console.error("❌ Missing required environment variables!");
    console.error("💡 Copy .env.example to .env and set the required values.");
    process.exit(1);
  } else {
    console.log("✅ All required environment variables are set!");
  }
}

/**
 * Show help information
 */
function showHelp() {
  console.log("Oracle to PostgreSQL Migration Tool");
  console.log("==================================");
  console.log("");
  console.log("Usage:");
  console.log(
    "  npm run migrate:dev          Run migration in development mode",
  );
  console.log("  npm run migrate              Build and run migration");
  console.log("  npx ts-node src/cli.ts <cmd> Run CLI commands");
  console.log("");
  console.log("CLI Commands:");
  console.log("  config, check-config         Check configuration validity");
  console.log("  env, check-env              Check environment variables");
  console.log("  help                        Show this help message");
  console.log("");
  console.log("Setup:");
  console.log("  1. Copy .env.example to .env");
  console.log("  2. Update .env with your database credentials");
  console.log('  3. Run "npm run migrate:dev" to start migration');
  console.log("");
  console.log("Environment File (.env):");
  console.log("  ORACLE_HOST=your_oracle_host");
  console.log("  ORACLE_SERVICE_NAME=your_service_name");
  console.log("  ORACLE_USER=your_oracle_user");
  console.log("  ORACLE_PASSWORD=your_oracle_password");
  console.log("  POSTGRES_HOST=your_postgres_host");
  console.log("  POSTGRES_DATABASE=your_postgres_db");
  console.log("  POSTGRES_USER=your_postgres_user");
  console.log("  POSTGRES_PASSWORD=your_postgres_password");
  console.log("");
}
