import { safeString, safeInteger, safeBoolean } from "../utils/data-helpers.js";
import { EnhancedTransformFunction } from "../types/index.js";
import { logInfo, logWarn, logError } from "../utils/logger.js";

/**
 * Debug version of district transformer with extensive logging
 */
export const debugDistrictTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  try {
    await logInfo(
      `🔍 Processing district row: ${JSON.stringify(row, null, 2)}`,
    );

    // Check if resolvers are available
    if (!resolvers) {
      await logError("❌ No resolvers provided to district transformer");
      return {
        code: safeString(row.CODE),
        name: safeString(row.NAME),
        post_code: safeString(row.POST_CODE),
        active: safeBoolean(row.ACTIVE),
        uuid: safeString(row.ID?.toString()),
        region_id: null, // No resolver available
        created_at: new Date(),
        updated_at: new Date(),
        deleted: false,
      };
    }

    await logInfo(
      `📋 Available resolvers: ${Object.keys(resolvers).join(", ")}`,
    );

    // Check for region resolver
    if (!resolvers.resolveRegionId) {
      await logError("❌ resolveRegionId not found in resolvers");
      await logInfo(
        `🔍 Available resolver functions: ${Object.keys(resolvers).join(", ")}`,
      );
      return {
        code: safeString(row.CODE),
        name: safeString(row.NAME),
        post_code: safeString(row.POST_CODE),
        active: safeBoolean(row.ACTIVE),
        uuid: safeString(row.ID?.toString()),
        region_id: null,
        created_at: new Date(),
        updated_at: new Date(),
        deleted: false,
      };
    }

    // Determine which field contains the region code
    const regionCode =
      row.REGION_ID || row.REGION_CODE || row.region_id || row.region_code;
    await logInfo(`🔍 Attempting to resolve region code: '${regionCode}'`);

    if (!regionCode) {
      await logWarn("⚠️ No region code found in row");
      await logInfo(`📋 Available row fields: ${Object.keys(row).join(", ")}`);

      // Log some sample field values
      Object.keys(row).forEach((key) => {
        if (key.toLowerCase().includes("region")) {
          logInfo(`   🔍 ${key}: '${row[key]}'`);
        }
      });
    }

    // Resolve the region ID
    const regionId = regionCode
      ? await resolvers.resolveRegionId(regionCode)
      : null;

    if (regionCode && !regionId) {
      await logError(
        `❌ Failed to resolve region code '${regionCode}' to region_id`,
      );
    } else if (regionId) {
      await logInfo(
        `✅ Successfully resolved region code '${regionCode}' -> region_id: ${regionId}`,
      );
    }

    const result = {
      code: safeString(row.CODE),
      name: safeString(row.NAME),
      post_code: safeString(row.POST_CODE),
      active: safeBoolean(row.ACTIVE),
      uuid: safeString(row.ID?.toString()),
      region_id: regionId,
      created_at: new Date(),
      updated_at: new Date(),
      deleted: false,
    };

    await logInfo(
      `✅ Final district record: ${JSON.stringify(result, null, 2)}`,
    );
    return result;
  } catch (error) {
    await logError(`❌ Error in district transformer: ${error}`);
    throw error;
  }
};

/**
 * Debug version of council transformer with extensive logging
 */
export const debugCouncilTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  try {
    await logInfo(`🔍 Processing council row: ${JSON.stringify(row, null, 2)}`);

    if (!resolvers?.resolveDistrictId) {
      await logError("❌ resolveDistrictId not found in resolvers");
      return {
        code: safeString(row.CODE),
        name: safeString(row.NAME),
        post_code: safeString(row.POST_CODE),
        mvc_uuid: safeString(row.MVC_UUID),
        active: safeBoolean(row.ACTIVE),
        uuid: safeString(row.ID?.toString()),
        district_id: null,
        district: safeString(row.DISTRICT_ID || row.DISTRICT_CODE),
        created_at: new Date(),
        updated_at: new Date(),
        deleted: false,
      };
    }

    const districtCode =
      row.DISTRICT_ID ||
      row.DISTRICT_CODE ||
      row.district_id ||
      row.district_code;
    await logInfo(`🔍 Attempting to resolve district code: '${districtCode}'`);

    const districtId = districtCode
      ? await resolvers.resolveDistrictId(districtCode)
      : null;

    if (districtCode && !districtId) {
      await logError(
        `❌ Failed to resolve district code '${districtCode}' to district_id`,
      );
    } else if (districtId) {
      await logInfo(
        `✅ Successfully resolved district code '${districtCode}' -> district_id: ${districtId}`,
      );
    }

    const result = {
      code: safeString(row.CODE),
      name: safeString(row.NAME),
      post_code: safeString(row.POST_CODE),
      mvc_uuid: safeString(row.MVC_UUID),
      active: safeBoolean(row.ACTIVE),
      uuid: safeString(row.ID?.toString()),
      district_id: districtId,
      district: safeString(districtCode),
      created_at: new Date(),
      updated_at: new Date(),
      deleted: false,
    };

    await logInfo(
      `✅ Final council record: ${JSON.stringify(result, null, 2)}`,
    );
    return result;
  } catch (error) {
    await logError(`❌ Error in council transformer: ${error}`);
    throw error;
  }
};

/**
 * Debug version of ward transformer with extensive logging
 */
export const debugWardTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  try {
    await logInfo(`🔍 Processing ward row: ${JSON.stringify(row, null, 2)}`);

    if (!resolvers) {
      await logError("❌ No resolvers provided to ward transformer");
      return {
        ward_id: safeInteger(row.ID),
        code: safeString(row.CODE),
        ward_name: safeString(row.NAME),
        post_code: safeString(row.POST_CODE),
        is_active: safeBoolean(row.ACTIVE),
        active: safeBoolean(row.ACTIVE),
        uuid: safeString(row.ID?.toString()),
        council_id: null,
        district: null,
        council: safeString(row.COUNCIL_ID || row.COUNCIL_CODE),
        council_name: safeString(row.COUNCIL_ID || row.COUNCIL_CODE),
        created_at: new Date(),
        updated_at: new Date(),
        deleted: false,
      };
    }

    const councilCode =
      row.COUNCIL_ID || row.COUNCIL_CODE || row.council_id || row.council_code;
    const districtCode =
      row.DISTRICT_ID ||
      row.DISTRICT_CODE ||
      row.district_id ||
      row.district_code;

    await logInfo(`🔍 Attempting to resolve council code: '${councilCode}'`);
    await logInfo(`🔍 Attempting to resolve district code: '${districtCode}'`);

    const councilId =
      councilCode && resolvers.resolveCouncilId
        ? await resolvers.resolveCouncilId(councilCode)
        : null;

    const districtId =
      districtCode && resolvers.resolveDistrictId
        ? await resolvers.resolveDistrictId(districtCode)
        : null;

    const result = {
      ward_id: safeInteger(row.ID),
      code: safeString(row.CODE),
      ward_name: safeString(row.NAME),
      post_code: safeString(row.POST_CODE),
      is_active: safeBoolean(row.ACTIVE),
      active: safeBoolean(row.ACTIVE),
      uuid: safeString(row.ID?.toString()),
      council_id: councilId,
      district: districtId,
      council: safeString(councilCode),
      council_name: safeString(councilCode),
      created_at: new Date(),
      updated_at: new Date(),
      deleted: false,
    };

    await logInfo(`✅ Final ward record: ${JSON.stringify(result, null, 2)}`);
    return result;
  } catch (error) {
    await logError(`❌ Error in ward transformer: ${error}`);
    throw error;
  }
};

/**
 * Helper function to test and debug cache contents
 */
export const debugCacheContents = async (resolvers: any) => {
  if (!resolvers) {
    await logError("❌ No resolvers provided for cache debugging");
    return;
  }

  await logInfo("🔍 Debugging cache contents...");

  // Test region resolver
  if (resolvers.resolveRegionId) {
    try {
      // Try a few common region codes
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
      await logInfo("🧪 Testing region resolver with common codes...");

      for (const code of testCodes) {
        const result = await resolvers.resolveRegionId(code);
        if (result) {
          await logInfo(`✅ Region cache test: '${code}' -> ${result}`);
          break;
        } else {
          await logWarn(`❌ Region cache test failed: '${code}' -> null`);
        }
      }
    } catch (error) {
      await logError(`❌ Error testing region cache: ${error}`);
    }
  } else {
    await logError("❌ resolveRegionId not found in resolvers");
  }

  if (resolvers.resolveDistrictId) {
    try {
      const testCodes = ["01", "02", "03", "1", "2", "3"];
      await logInfo("🧪 Testing district resolver with common codes...");

      for (const code of testCodes) {
        const result = await resolvers.resolveDistrictId(code);
        if (result) {
          await logInfo(`✅ District cache test: '${code}' -> ${result}`);
          break;
        } else {
          await logWarn(`❌ District cache test failed: '${code}' -> null`);
        }
      }
    } catch (error) {
      await logError(`❌ Error testing district cache: ${error}`);
    }
  } else {
    await logError("❌ resolveDistrictId not found in resolvers");
  }
};
