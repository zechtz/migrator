import { createConfig, type ConfigOptions } from "config";
import { runMigration, runSequentialMigration } from "migration/runner";
import { type MigrationTask } from "types";
import { createFieldMapper, createCustomTransformer } from "data/transform";

const main = async (): Promise<void> => {
  const configOptions: ConfigOptions = {
    // Database connections
    oracleHost: "oracle-server.example.com",
    oraclePort: 1521,
    oracleServiceName: "ORCL",
    oracleUser: "your_oracle_user",
    oraclePassword: "your_oracle_password",
    postgresHost: "postgres-server.example.com",
    postgresPort: 5432,
    postgresDatabase: "your_postgres_db",
    postgresUser: "your_postgres_user",
    postgresPassword: "your_postgres_password",

    // Basic settings
    batchSize: 1000,
    maxRetries: 3,
    retryDelay: 5000,

    // Concurrency settings
    maxConcurrentBatches: 4, // Process up to 4 batches simultaneously
    maxConcurrentTables: 2, // Process up to 2 tables simultaneously
    maxConcurrentConnections: 10, // PostgreSQL connection pool size

    // Pagination strategy (global default)
    paginationStrategy: "cursor", // Options: 'rownum', 'offset', 'cursor'
    cursorColumn: "id", // Default cursor column for cursor-based pagination
  };

  const config = createConfig(configOptions);

  // Example transformations
  const userTransform = createFieldMapper({
    USER_ID: "id",
    FULL_NAME: "name",
    EMAIL_ADDRESS: "email",
    CREATED_DATE: "created_at",
  });

  const orderTransform = createCustomTransformer((row: any) => ({
    id: row.ORDER_ID,
    customer_id: row.CUSTOMER_ID,
    total: parseFloat(row.TOTAL_AMOUNT),
    status: row.ORDER_STATUS?.toLowerCase(),
    created_at: new Date(row.ORDER_DATE),
    is_large_order: parseFloat(row.TOTAL_AMOUNT) > 1000,
  }));

  const migrations: MigrationTask[] = [
    {
      sourceQuery: "SELECT * FROM users ORDER BY user_id",
      targetTable: "users",
      transformFn: userTransform,
      priority: 10, // High priority - migrate first
      maxConcurrentBatches: 6, // Override global setting for this table
      paginationStrategy: "cursor",
      cursorColumn: "user_id",
      orderByClause: "ORDER BY user_id ASC",
    },
    {
      sourceQuery: "SELECT * FROM orders ORDER BY order_id",
      targetTable: "orders",
      transformFn: orderTransform,
      priority: 5, // Medium priority
      paginationStrategy: "cursor",
      cursorColumn: "order_id",
      orderByClause: "ORDER BY order_id ASC",
    },
    {
      sourceQuery: "SELECT * FROM products ORDER BY product_id",
      targetTable: "products",
      priority: 1, // Low priority - migrate last
      paginationStrategy: "offset", // Use OFFSET for this table
      orderByClause: "ORDER BY product_id ASC",
    },
    {
      sourceQuery: "SELECT * FROM simple_table ORDER BY id",
      targetTable: "simple_table",
      // Uses default transformation and pagination strategy
    },
  ];

  try {
    // Choose migration type
    const useConcurrentMigration = true;

    if (useConcurrentMigration) {
      console.log("Starting concurrent migration...");
      await runMigration(config, migrations);
    } else {
      console.log("Starting sequential migration...");
      await runSequentialMigration(config, migrations);
    }

    console.log("Migration completed successfully!");
    process.exit(0);
  } catch (error) {
    console.error("Migration failed:", (error as Error).message);
    process.exit(1);
  }
};

// Graceful shutdown handlers
process.on("SIGINT", () => {
  console.log("\nReceived SIGINT. Shutting down gracefully...");
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("\nReceived SIGTERM. Shutting down gracefully...");
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error);
}
