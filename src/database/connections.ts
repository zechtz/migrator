import { DatabaseConnections, MigrationConfig } from "../../src/types/index.js";
import { connectToOracle, closeOracleConnection } from "./oracle.js";
import { connectToPostgres, closePostgresPool } from "./postgres.js";

export const initializeConnections = async (
  config: MigrationConfig,
): Promise<DatabaseConnections> => {
  const oracle = await connectToOracle(config.oracle);
  const postgresPool = await connectToPostgres(config.postgres);

  return { oracle, postgresPool };
};

export const closeConnections = async (
  connections: DatabaseConnections,
): Promise<void> => {
  await Promise.all([
    closeOracleConnection(connections.oracle),
    closePostgresPool(connections.postgresPool),
  ]);
};
