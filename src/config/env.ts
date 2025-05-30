import { config } from "dotenv";
import { resolve } from "path";
import { ConfigOptions } from "./index.js";

// Load environment variables from .env file
config({ path: resolve(process.cwd(), ".env") });

/**
 * Validates that a required environment variable exists
 */
function requireEnv(key: string): string {
  const value = process.env[key];
  if (!value) {
    throw new Error(`Required environment variable ${key} is not set`);
  }
  return value;
}

/**
 * Gets an optional environment variable with a default value
 */
function getEnv(key: string, defaultValue: string): string;
function getEnv(key: string, defaultValue: number): number;
function getEnv(key: string, defaultValue: boolean): boolean;
function getEnv(key: string, defaultValue: any): any {
  const value = process.env[key];

  if (!value) {
    return defaultValue;
  }

  // Handle different types
  if (typeof defaultValue === "number") {
    const parsed = parseInt(value, 10);
    if (isNaN(parsed)) {
      console.warn(
        `Invalid number for ${key}: ${value}, using default: ${defaultValue}`,
      );
      return defaultValue;
    }
    return parsed;
  }

  if (typeof defaultValue === "boolean") {
    return value.toLowerCase() === "true";
  }

  return value;
}

/**
 * Validates pagination strategy
 */
function validatePaginationStrategy(
  strategy: string,
): "rownum" | "offset" | "cursor" {
  const validStrategies = ["rownum", "offset", "cursor"] as const;
  if (validStrategies.includes(strategy as any)) {
    return strategy as "rownum" | "offset" | "cursor";
  }

  console.warn(
    `Invalid pagination strategy: ${strategy}, defaulting to 'rownum'`,
  );
  return "rownum";
}

/**
 * Creates configuration from environment variables
 */
export function createConfigFromEnv(): ConfigOptions {
  try {
    // Validate Oracle connection method
    const oracleSid = process.env.ORACLE_SID;
    const oracleServiceName = process.env.ORACLE_SERVICE_NAME;

    if (!oracleSid && !oracleServiceName) {
      throw new Error("Either ORACLE_SID or ORACLE_SERVICE_NAME must be set");
    }

    if (oracleSid && oracleServiceName) {
      console.warn(
        "⚠️  Both ORACLE_SID and ORACLE_SERVICE_NAME are set. Using ORACLE_SID.",
      );
    }

    const configOptions: ConfigOptions = {
      // Oracle Database (required)
      oracleHost: requireEnv("ORACLE_HOST"),
      oraclePort: getEnv("ORACLE_PORT", 1521),
      oracleServiceName: oracleSid || oracleServiceName!, // Use SID if available, otherwise service name
      oracleSid: getEnv("ORACLE_SID", ""),
      oracleUser: requireEnv("ORACLE_USER"),
      oraclePassword: requireEnv("ORACLE_PASSWORD"),

      // PostgreSQL Database (required)
      postgresHost: requireEnv("POSTGRES_HOST"),
      postgresPort: getEnv("POSTGRES_PORT", 5432),
      postgresDatabase: requireEnv("POSTGRES_DATABASE"),
      postgresUser: requireEnv("POSTGRES_USER"),
      postgresPassword: requireEnv("POSTGRES_PASSWORD"),

      // Migration Settings
      batchSize: getEnv("BATCH_SIZE", 1000),
      maxRetries: getEnv("MAX_RETRIES", 3),
      retryDelay: getEnv("RETRY_DELAY", 5000),
      checkpointFile: getEnv("CHECKPOINT_FILE", "migration_checkpoint.json"),

      // Concurrency Settings
      maxConcurrentBatches: getEnv("MAX_CONCURRENT_BATCHES", 4),
      maxConcurrentTables: getEnv("MAX_CONCURRENT_TABLES", 2),
      maxConcurrentConnections: getEnv("MAX_CONCURRENT_CONNECTIONS", 10),

      // Pagination Settings
      paginationStrategy: validatePaginationStrategy(
        getEnv("PAGINATION_STRATEGY", "rownum"),
      ),
      cursorColumn: process.env.CURSOR_COLUMN,
    };

    return configOptions;
  } catch (error) {
    console.error("❌ Configuration Error:", (error as Error).message);
    console.error(
      "Please check your .env file and ensure all required variables are set.",
    );
    console.error(
      "Copy .env.example to .env and update with your database credentials.",
    );
    process.exit(1);
  }
}

/**
 * Validates the configuration and shows warnings for potential issues
 */
export function validateConfiguration(config: ConfigOptions): void {
  console.log("🔧 Configuration Summary:");

  // Show Oracle connection info based on SID vs Service Name
  const oracleConnectionInfo = config.oracleSid
    ? `${config.oracleHost}:${config.oraclePort || 1521}/${config.oracleSid} (SID)`
    : `${config.oracleHost}:${config.oraclePort || 1521}/${config.oracleServiceName || "unknown"} (Service)`;

  console.log(`   Oracle: ${oracleConnectionInfo}`);
  console.log(
    `   PostgreSQL: ${config.postgresHost}:${config.postgresPort || 5432}/${config.postgresDatabase}`,
  );
  console.log(`   Batch Size: ${config.batchSize || 1000}`);
  console.log(`   Pagination: ${config.paginationStrategy || "rownum"}`);
  console.log(
    `   Concurrency: ${config.maxConcurrentTables || 2} tables, ${config.maxConcurrentBatches || 4} batches`,
  );
  console.log("");

  // Warnings - with proper null checks
  const batchSize = config.batchSize || 1000;
  if (batchSize > 10000) {
    console.warn(
      "⚠️  Large batch size detected. Consider using smaller batches for better memory usage.",
    );
  }

  const maxConcurrentBatches = config.maxConcurrentBatches || 4;
  if (maxConcurrentBatches > 10) {
    console.warn(
      "⚠️  High concurrency detected. Monitor database connections and performance.",
    );
  }

  if (config.paginationStrategy === "cursor" && !config.cursorColumn) {
    console.warn(
      "⚠️  Cursor pagination selected but no cursor column specified. Will auto-detect or fallback to rownum.",
    );
  }
}
