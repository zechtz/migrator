import { MigrationConfig } from "types";

export interface ConfigOptions {
  oracleHost: string;
  oraclePort?: number;
  oracleServiceName?: string;
  oracleSid?: string;
  oracleUser: string;
  oraclePassword: string;
  postgresHost: string;
  postgresPort?: number;
  postgresDatabase: string;
  postgresUser: string;
  postgresPassword: string;
  batchSize?: number;
  maxRetries?: number;
  retryDelay?: number;
  checkpointFile?: string;
  maxConcurrentConnections?: number;
  maxConcurrentBatches?: number;
  maxConcurrentTables?: number;
  paginationStrategy?: "rownum" | "offset" | "cursor";
  cursorColumn?: string;
}

export const createConfig = (options: ConfigOptions): MigrationConfig => {
  const {
    oracleHost,
    oraclePort = 1521,
    oracleServiceName,
    oracleSid,
    oracleUser,
    oraclePassword,
    postgresHost,
    postgresPort = 5432,
    postgresDatabase,
    postgresUser,
    postgresPassword,
    batchSize = 1000,
    maxRetries = 3,
    retryDelay = 5000,
    checkpointFile = "migration_checkpoint.json",
    maxConcurrentConnections = 10,
    maxConcurrentBatches = 4,
    maxConcurrentTables = 2,
    paginationStrategy = "rownum",
    cursorColumn,
  } = options;

  // Validation: ensure either SID or Service Name is provided
  if (!oracleSid && !oracleServiceName) {
    throw new Error("Either oracleSid or oracleServiceName must be provided");
  }

  return {
    oracle: {
      host: oracleHost,
      port: oraclePort,
      serviceName: oracleServiceName,
      sid: oracleSid,
      user: oracleUser,
      password: oraclePassword,
    },
    postgres: {
      host: postgresHost,
      port: postgresPort,
      database: postgresDatabase,
      user: postgresUser,
      password: postgresPassword,
      max: maxConcurrentConnections,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    },
    batchSize,
    maxRetries,
    retryDelay,
    checkpointFile,
    maxConcurrentBatches,
    maxConcurrentTables,
    paginationStrategy,
    cursorColumn,
  };
};
