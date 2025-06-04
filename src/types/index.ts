import { ForeignKeyResolver } from "../utils/foreign-key-resolver.js";

export interface OracleConfig {
  host: string;
  port: number;
  serviceName?: string;
  sid?: string;
  user: string;
  password: string;
}

export interface PostgresConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  max?: number;
  idleTimeoutMillis?: number;
  connectionTimeoutMillis?: number;
}

export interface MigrationConfig {
  oracle: OracleConfig;
  postgres: PostgresConfig;
  batchSize: number;
  maxRetries: number;
  retryDelay: number;
  checkpointFile: string;
  // Concurrency settings
  maxConcurrentBatches: number;
  maxConcurrentTables: number;
  // Pagination settings
  paginationStrategy: "rownum" | "offset" | "cursor";
  cursorColumn?: string;
}

export interface Checkpoint {
  lastProcessedId: number;
  totalProcessed: number;
  lastUpdate?: string;
  tableProgress?: Record<string, TableProgress>;
}

export interface TableProgress {
  lastProcessedId: number;
  totalProcessed: number;
  isComplete: boolean;
  lastCursorValue?: any; // For cursor-based pagination
}

/**
 * Cache dependency configuration
 */
export interface CacheDependency {
  tableName: string;
  codeColumn?: string;
  idColumn?: string;
}

export interface MigrationTask {
  sourceQuery: string;
  targetTable: string;
  transformFn?: TransformFunction | EnhancedTransformFunction;
  priority?: number;
  maxConcurrentBatches?: number;
  paginationStrategy?: "rownum" | "offset" | "cursor";
  cursorColumn?: string;
  orderByClause?: string;

  // Update/Upsert control
  migrationMode?: "insert" | "upsert" | "update" | "delete-insert";
  conflictColumns?: string[]; // Columns to check for conflicts (e.g., ['country_code'])
  updateConditions?: string; // Additional WHERE conditions for updates
  updateColumns?: string[]; // Specific columns to update (if not specified, updates all)
  onConflict?: "update" | "ignore" | "error"; // What to do on conflict

  // Foreign key resolution support
  requiredResolvers?: string[];
  cacheDependencies?: CacheDependency[];
}

export interface DatabaseConnections {
  oracle: any; // oracledb.Connection
  postgresPool: any; // Pool
}

export interface BatchResult {
  finished: boolean;
  processedCount: number;
  lastCursorValue?: any;
}

export interface BatchJob {
  tableId: string;
  batchNumber: number;
  offset: number;
  batchSize: number;
  sourceQuery: string;
  targetTable: string;
  transformFn?: TransformFunction | EnhancedTransformFunction;
  paginationStrategy: "rownum" | "offset" | "cursor";
  cursorColumn?: string;
  lastCursorValue?: any;
}

/**
 * Original transform function (synchronous, no resolvers)
 */
export type TransformFunction = (row: any) => Record<string, any>;

/**
 * Enhanced transform function that receives resolvers as second parameter
 */
export type EnhancedTransformFunction = (
  row: any,
  resolvers?: Record<string, ForeignKeyResolver>,
) => Promise<Record<string, any>> | Record<string, any>;

export interface ConcurrencyManager {
  tableSemaphore: any; // Semaphore for table-level concurrency
  batchSemaphore: any; // Semaphore for batch-level concurrency
}

export interface PaginationContext {
  strategy: "rownum" | "offset" | "cursor";
  offset: number;
  batchSize: number;
  cursorColumn?: string;
  lastCursorValue?: any;
  orderByClause?: string;
}

/**
 * Enhanced Migration Task - extends MigrationTask with additional features
 * Note: This is now merged into MigrationTask for simplicity
 */
export interface EnhancedMigrationTask extends MigrationTask {}
