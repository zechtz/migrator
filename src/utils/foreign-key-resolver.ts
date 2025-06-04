import { Pool } from "pg";
import { logInfo, logError, logWarn } from "./logger.js";

// Global mapping cache
const mappingCache = new Map<string, Map<string, number>>();

/**
 * Generic foreign key resolver callback type
 */
export type ForeignKeyResolver = (sourceCode: string) => Promise<number | null>;

/**
 * Build mapping cache for any table
 */
export const buildMappingCache = async (
  pgPool: Pool,
  tableName: string,
  codeColumn: string = "code",
  idColumn: string = "id",
): Promise<void> => {
  try {
    const client = await pgPool.connect();

    const query = `SELECT ${codeColumn}, ${idColumn} FROM ${tableName} WHERE ${codeColumn} IS NOT NULL`;
    const result = await client.query(query);

    const tableMap = new Map<string, number>();
    result.rows.forEach((row) => {
      tableMap.set(row[codeColumn], row[idColumn]);
    });

    mappingCache.set(tableName, tableMap);
    await logInfo(
      `Built mapping cache for ${tableName}: ${tableMap.size} entries`,
    );

    client.release();
  } catch (error) {
    await logError(`Failed to build mapping cache for ${tableName}: ${error}`);
    throw error;
  }
};

/**
 * Create a foreign key resolver for any table
 */
export const createForeignKeyResolver = (
  tableName: string,
  codeColumn: string = "code",
): ForeignKeyResolver => {
  return async (sourceCode: string): Promise<number | null> => {
    if (!sourceCode) return null;

    const tableMap = mappingCache.get(tableName);
    if (!tableMap) {
      await logWarn(`Mapping cache not found for table: ${tableName}`);
      return null;
    }

    return tableMap.get(sourceCode) || null;
  };
};

/**
 * Create multiple foreign key resolvers at once
 */
export const createResolvers = (
  configs: Array<{
    name: string;
    tableName: string;
    codeColumn?: string;
  }>,
): Record<string, ForeignKeyResolver> => {
  const resolvers: Record<string, ForeignKeyResolver> = {};

  configs.forEach((config) => {
    resolvers[config.name] = createForeignKeyResolver(
      config.tableName,
      config.codeColumn || "code",
    );
  });

  return resolvers;
};

/**
 * Pre-defined resolver configurations for common tables
 */
export const COMMON_RESOLVERS = {
  region: {
    name: "resolveRegionId",
    tableName: "crvs_global.tbl_delimitation_region",
    codeColumn: "code",
  },
  district: {
    name: "resolveDistrictId",
    tableName: "crvs_global.tbl_delimitation_district",
    codeColumn: "code",
  },
  council: {
    name: "resolveCouncilId",
    tableName: "crvs_global.tbl_delimitation_council",
    codeColumn: "code",
  },
  ward: {
    name: "resolveWardId",
    tableName: "crvs_global.tbl_delimitation_ward",
    codeColumn: "code",
  },
  healthFacility: {
    name: "resolveHealthFacilityId",
    tableName: "crvs_global.tbl_mgt_health_facility",
    codeColumn: "code",
  },
};
