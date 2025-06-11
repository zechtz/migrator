import { Pool } from "pg";
import { logInfo, logError, logWarn } from "./logger.js";
import { CacheConfig } from "../types//index.js";

const mappingCache = new Map<string, Map<string, number>>();

export type ForeignKeyResolver = (sourceCode: string) => Promise<number | null>;

/**
 * Default cache configurations
 */
export const DEFAULT_CACHE_CONFIGS: Record<string, CacheConfig> = {
  "crvs_global.tbl_delimitation_region": {
    tableName: "crvs_global.tbl_delimitation_region",
    codeCol: "code",
    idCol: "id",
  },
  "crvs_global.tbl_delimitation_district": {
    tableName: "crvs_global.tbl_delimitation_district",
    codeCol: "code",
    idCol: "id",
    dependsOn: ["crvs_global.tbl_delimitation_region"],
  },
  "crvs_global.tbl_delimitation_council": {
    tableName: "crvs_global.tbl_delimitation_council",
    codeCol: "code",
    idCol: "id",
    dependsOn: ["crvs_global.tbl_delimitation_district"],
  },
  "crvs_global.tbl_delimitation_ward": {
    tableName: "crvs_global.tbl_delimitation_ward",
    codeCol: "code",
    idCol: "ward_id",
    dependsOn: ["crvs_global.tbl_delimitation_council"],
  },
  "crvs_global.tbl_mgt_health_facility": {
    tableName: "crvs_global.tbl_mgt_health_facility",
    codeCol: "code",
    idCol: "health_facility_id",
    dependsOn: ["crvs_global.tbl_delimitation_council"],
  },
};

/**
 * Check if table exists and has the required columns
 */
const validateTableForCache = async (
  pgPool: Pool,
  tableName: string,
  codeColumn: string,
  idColumn: string,
): Promise<{ exists: boolean; hasData: boolean; missingColumns: string[] }> => {
  const client = await pgPool.connect();

  try {
    // Check if table exists
    const tableExistsQuery = `
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = split_part($1, '.', 1) 
        AND table_name = split_part($1, '.', 2)
      )
    `;

    const tableExistsResult = await client.query(tableExistsQuery, [tableName]);

    if (!tableExistsResult.rows[0].exists) {
      return { exists: false, hasData: false, missingColumns: [] };
    }

    // Check if columns exist
    const columnsQuery = `
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_schema = split_part($1, '.', 1) 
      AND table_name = split_part($1, '.', 2)
      AND column_name IN ($2, $3)
    `;

    const columnsResult = await client.query(columnsQuery, [
      tableName,
      codeColumn,
      idColumn,
    ]);

    const availableColumns = columnsResult.rows.map((row) => row.column_name);
    const missingColumns = [codeColumn, idColumn].filter(
      (col) => !availableColumns.includes(col),
    );

    if (missingColumns.length > 0) {
      return { exists: true, hasData: false, missingColumns };
    }

    // Check if table has data
    const dataQuery = `SELECT COUNT(*) FROM ${tableName} WHERE ${codeColumn} IS NOT NULL LIMIT 1`;
    const dataResult = await client.query(dataQuery);
    const hasData = parseInt(dataResult.rows[0].count) > 0;

    return { exists: true, hasData, missingColumns: [] };
  } finally {
    client.release();
  }
};

/**
 *  cache building with better error handling and logging
 */
export const buildMappingCacheEnhanced = async (
  pgPool: Pool,
  tableName: string,
  codeColumn: string = "code",
  idColumn: string = "id",
): Promise<boolean> => {
  try {
    await logInfo(`🏗️ Building cache for ${tableName}...`);

    const validation = await validateTableForCache(
      pgPool,
      tableName,
      codeColumn,
      idColumn,
    );

    if (!validation.exists) {
      await logWarn(
        `⚠️ Table ${tableName} does not exist, skipping cache build`,
      );
      return false;
    }

    if (validation.missingColumns.length > 0) {
      await logError(
        `❌ Missing columns in ${tableName}: ${validation.missingColumns.join(", ")}`,
      );
      return false;
    }

    if (!validation.hasData) {
      await logWarn(`⚠️ Table ${tableName} has no data, skipping cache build`);
      return false;
    }

    const client = await pgPool.connect();

    try {
      const query = `
        SELECT ${codeColumn}, ${idColumn} 
        FROM ${tableName} 
        WHERE ${codeColumn} IS NOT NULL 
        AND ${idColumn} IS NOT NULL
      `;

      const result = await client.query(query);

      const tableMap = new Map<string, number>();
      result.rows.forEach((row) => {
        const code = row[codeColumn];
        const id = row[idColumn];
        tableMap.set(String(code), Number(id));
      });

      mappingCache.set(tableName, tableMap);

      await logInfo(
        `✅ Built cache for ${tableName}: ${tableMap.size} entries (${codeColumn} -> ${idColumn})`,
      );

      // Log sample entries for debugging
      if (tableMap.size > 0) {
        const sample = Array.from(tableMap.entries()).slice(0, 3);
        await logInfo(
          `   📋 Sample mappings: ${sample.map(([k, v]) => `${k}->${v}`).join(", ")}`,
        );
      }

      return true;
    } finally {
      client.release();
    }
  } catch (error) {
    await logError(`❌ Failed to build cache for ${tableName}: ${error}`);
    return false;
  }
};

/**
 * foreign key resolver with debugging
 */
export const createForeignKeyResolverEnhanced = (
  tableName: string,
  codeColumn: string = "code",
): ForeignKeyResolver => {
  return async (sourceCode: string): Promise<number | null> => {
    if (!sourceCode) {
      await logWarn(`⚠️ Empty source code provided for ${tableName} resolver`);
      return null;
    }

    const tableMap = mappingCache.get(tableName);
    if (!tableMap) {
      await logError(`❌ Cache not found for table: ${tableName}`);
      await logInfo(
        `📋 Available caches: ${Array.from(mappingCache.keys()).join(", ")}`,
      );
      return null;
    }

    const result = tableMap.get(String(sourceCode));

    if (result === undefined) {
      await logWarn(`⚠️ Code '${sourceCode}' not found in ${tableName} cache`);
      // Log available codes for debugging (first 10)
      const availableCodes = Array.from(tableMap.keys()).slice(0, 10);
      await logWarn(
        `   📋 Available codes (sample): ${availableCodes.join(", ")}`,
      );
      return null;
    }

    return result;
  };
};

/**
 * Build initial caches for all reference tables that already exist
 */
export const buildInitialCaches = async (
  pgPool: Pool,
  cacheConfigs: Record<string, CacheConfig> = DEFAULT_CACHE_CONFIGS,
): Promise<void> => {
  await logInfo("🏗️ Building initial foreign key caches...");

  for (const [tableName, config] of Object.entries(cacheConfigs)) {
    const success = await buildMappingCacheEnhanced(
      pgPool,
      tableName,
      config.codeCol,
      config.idCol,
    );

    if (success) {
      await logInfo(`✅ Built initial cache for ${tableName}`);
    } else {
      await logWarn(`⚠️ Skipped cache for ${tableName}`);
    }
  }
};

/**
 * Build cache after a table migration completes
 */
export const buildCacheAfterMigration = async (
  pgPool: Pool,
  targetTable: string,
  cacheConfigs: Record<string, CacheConfig> = DEFAULT_CACHE_CONFIGS,
): Promise<void> => {
  const config = cacheConfigs[targetTable];
  if (!config) {
    return; // Not a reference table
  }

  try {
    const success = await buildMappingCacheEnhanced(
      pgPool,
      targetTable,
      config.codeCol,
      config.idCol,
    );

    if (success) {
      await logInfo(`✅ Built cache for newly migrated table: ${targetTable}`);
    } else {
      await logError(`❌ Failed to build cache for ${targetTable}`);
      throw new Error(`Cache building failed for ${targetTable}`);
    }
  } catch (error) {
    await logError(`❌ Failed to build cache for ${targetTable}: ${error}`);
    throw error;
  }
};

/**
 * Create resolvers for enhanced transform functions
 */
export const createResolversEnhanced = (
  configs: Array<{
    name: string;
    tableName: string;
    codeColumn?: string;
  }>,
): Record<string, ForeignKeyResolver> => {
  const resolvers: Record<string, ForeignKeyResolver> = {};

  configs.forEach((config) => {
    resolvers[config.name] = createForeignKeyResolverEnhanced(
      config.tableName,
      config.codeColumn || "code",
    );
  });

  return resolvers;
};

/**
 * Determine which tables need foreign key resolvers
 */
export const getTablesNeedingResolvers = (): string[] => {
  return [
    "crvs_global.tbl_delimitation_district",
    "crvs_global.tbl_delimitation_council",
    "crvs_global.tbl_delimitation_ward",
    "crvs_global.tbl_mgt_health_facility",
    "crvs_global.tbl_mgt_registration_center",
    "registry.tbl_birth_certificate_info",
  ];
};

/**
 * Check cache contents for debugging
 */
export const debugCacheContents = async (
  resolvers: Record<string, ForeignKeyResolver>,
): Promise<void> => {
  if (!resolvers) {
    await logError("❌ No resolvers provided for cache debugging");
    return;
  }

  await logInfo("🔍 Debugging cache contents...");

  // Test region resolver
  if (resolvers.resolveRegionId) {
    try {
      const testCodes = [
        "01",
        "02",
        "03",
        "1",
        "2",
        "3",
        "DAR",
        "ARUSHA",
        "DODOMA",
      ];
      for (const code of testCodes) {
        const result = await resolvers.resolveRegionId(code);
        if (result) {
          await logInfo(`✅ Region cache test: '${code}' -> ${result}`);
          break;
        }
      }
    } catch (error) {
      await logError(`❌ Error testing region cache: ${error}`);
    }
  }

  // Test district resolver
  if (resolvers.resolveDistrictId) {
    try {
      const testCodes = ["01", "02", "03", "1", "2", "3"];
      for (const code of testCodes) {
        const result = await resolvers.resolveDistrictId(code);
        if (result) {
          await logInfo(`✅ District cache test: '${code}' -> ${result}`);
          break;
        }
      }
    } catch (error) {
      await logError(`❌ Error testing district cache: ${error}`);
    }
  }
};

/**
 * Enhanced cache configurations for birth registration
 */
export const BIRTH_REGISTRATION_CACHE_CONFIGS: Record<string, CacheConfig> = {
  // Location hierarchy
  "crvs_global.tbl_delimitation_region": {
    tableName: "crvs_global.tbl_delimitation_region",
    codeCol: "code",
    idCol: "id",
  },
  "crvs_global.tbl_delimitation_district": {
    tableName: "crvs_global.tbl_delimitation_district",
    codeCol: "code",
    idCol: "id",
    dependsOn: ["crvs_global.tbl_delimitation_region"],
  },
  "crvs_global.tbl_delimitation_ward": {
    tableName: "crvs_global.tbl_delimitation_ward",
    codeCol: "code",
    idCol: "ward_id",
    dependsOn: ["crvs_global.tbl_delimitation_council"],
  },

  // Countries
  "crvs_global.tbl_global_country": {
    tableName: "crvs_global.tbl_global_country",
    codeCol: "country_code",
    idCol: "id",
  },

  // Health facilities
  "crvs_global.tbl_mgt_health_facility": {
    tableName: "crvs_global.tbl_mgt_health_facility",
    codeCol: "code",
    idCol: "health_facility_id",
    dependsOn: ["crvs_global.tbl_delimitation_council"],
  },

  // Place of birth lookup
  "crvs_global.tbl_global_place_of_birth": {
    tableName: "crvs_global.tbl_global_place_of_birth",
    codeCol: "code",
    idCol: "id",
  },
};

/**
 * Enhanced resolvers configuration for birth registration
 */
export const BIRTH_REGISTRATION_RESOLVERS = [
  {
    name: "resolveRegionId",
    tableName: "crvs_global.tbl_delimitation_region",
    codeColumn: "code",
  },
  {
    name: "resolveDistrictId",
    tableName: "crvs_global.tbl_delimitation_district",
    codeColumn: "code",
  },
  {
    name: "resolveWardId",
    tableName: "crvs_global.tbl_delimitation_ward",
    codeColumn: "code",
  },
  {
    name: "resolveCountryId",
    tableName: "crvs_global.tbl_global_country",
    codeColumn: "country_code",
  },
  {
    name: "resolveHealthFacilityId",
    tableName: "crvs_global.tbl_mgt_health_facility",
    codeColumn: "code",
  },
  {
    name: "resolvePlaceOfBirthId",
    tableName: "crvs_global.tbl_global_place_of_birth",
    codeColumn: "code",
  },
];

/**
 * Create resolvers specifically for birth registration migration
 */

export const createBirthRegistrationResolvers = () => {
  return createResolversEnhanced(BIRTH_REGISTRATION_RESOLVERS);
};

export const buildMappingCache = buildMappingCacheEnhanced;
export const createForeignKeyResolver = createForeignKeyResolverEnhanced;
export const createResolvers = createResolversEnhanced;
