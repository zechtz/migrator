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
  // Enhanced checkpoint data
  tableProgress?: Record<string, TableProgress>;
}

export interface TableProgress {
  lastProcessedId: number;
  totalProcessed: number;
  isComplete: boolean;
  lastCursorValue?: any; // For cursor-based pagination
}

export interface MigrationTask {
  sourceQuery: string;
  targetTable: string;
  transformFn?: TransformFunction;
  // Concurrency settings per table
  priority?: number; // Higher number = higher priority
  maxConcurrentBatches?: number; // Override global setting
  // Pagination settings per table
  paginationStrategy?: "rownum" | "offset" | "cursor";
  cursorColumn?: string;
  orderByClause?: string; // For consistent pagination
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
  transformFn?: TransformFunction;
  paginationStrategy: "rownum" | "offset" | "cursor";
  cursorColumn?: string;
  lastCursorValue?: any;
}

export type TransformFunction = (row: any) => Record<string, any>;

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
