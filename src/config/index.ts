import { MigrationConfig } from "types";

export interface ConfigOptions {
  oracleHost: string;
  oraclePort?: number;
  oracleServiceName: string;
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

  return {
    oracle: {
      host: oracleHost,
      port: oraclePort,
      serviceName: oracleServiceName,
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
