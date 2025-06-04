import { safeString, safeInteger, safeBoolean } from "../utils/data-helpers.js";
import { EnhancedTransformFunction } from "../types/index.js";

/**
 * Generic district transformer - works for ANY district table
 */
export const districtTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  // Use resolver callback to get foreign key
  const regionId = resolvers?.resolveRegionId
    ? await resolvers.resolveRegionId(row.REGION_CODE)
    : null;

  return {
    code: safeString(row.CODE),
    name: safeString(row.NAME),
    post_code: safeString(row.POST_CODE),
    active: safeBoolean(row.ACTIVE),
    uuid: safeString(row.ID?.toString()),
    region_id: regionId, // ✅ Resolved via callback
    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
  };
};

/**
 * Generic council transformer - works for ANY council table
 */
export const councilTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  const districtId = resolvers?.resolveDistrictId
    ? await resolvers.resolveDistrictId(row.DISTRICT_CODE)
    : null;

  return {
    code: safeString(row.CODE),
    name: safeString(row.NAME),
    post_code: safeString(row.POST_CODE),
    mvc_uuid: safeString(row.MVC_UUID),
    active: safeBoolean(row.ACTIVE),
    uuid: safeString(row.ID?.toString()),
    district_id: districtId, // ✅ Resolved via callback
    district: safeString(row.DISTRICT_CODE),
    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
  };
};

/**
 * Generic ward transformer - works for ANY ward table
 */
export const wardTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  const councilId = resolvers?.resolveCouncilId
    ? await resolvers.resolveCouncilId(row.COUNCIL_CODE)
    : null;

  const districtId = resolvers?.resolveDistrictId
    ? await resolvers.resolveDistrictId(row.DISTRICT_CODE)
    : null;

  return {
    ward_id: safeInteger(row.ID),
    code: safeString(row.CODE),
    ward_name: safeString(row.NAME),
    post_code: safeString(row.POST_CODE),
    is_active: safeBoolean(row.ACTIVE),
    active: safeBoolean(row.ACTIVE),
    uuid: safeString(row.ID?.toString()),
    council_id: councilId, // ✅ Resolved via callback
    district: districtId, // ✅ Resolved via callback
    council: safeString(row.COUNCIL_CODE),
    council_name: safeString(row.COUNCIL_CODE),
    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
  };
};

/**
 * Generic registration center transformer - handles multiple foreign keys
 */
export const registrationCenterTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  // Resolve multiple foreign keys using different resolvers
  const regionId = resolvers?.resolveRegionId
    ? await resolvers.resolveRegionId(row.REGION_CODE)
    : null;

  const districtId = resolvers?.resolveDistrictId
    ? await resolvers.resolveDistrictId(row.DISTRICT_CODE)
    : null;

  const councilId = resolvers?.resolveCouncilId
    ? await resolvers.resolveCouncilId(row.COUNCIL_CODE)
    : null;

  const wardId = resolvers?.resolveWardId
    ? await resolvers.resolveWardId(row.WARD_CODE)
    : null;

  const healthFacilityId = resolvers?.resolveHealthFacilityId
    ? await resolvers.resolveHealthFacilityId(row.HEALTH_FACILITY_CODE)
    : null;

  return {
    code: safeString(row.CODE),
    center_code: safeString(row.CODE),
    name: safeString(row.NAME),
    registration_facility: safeBoolean(row.REGISTRATION_FACILITY),
    printing_facility: safeBoolean(row.PRINTING_FACILITY),
    active: safeBoolean(row.ACTIVE),
    head_office: safeString(row.HEAD_OFFICE),
    is_head_office: safeBoolean(row.HEAD_OFFICE === "Y"),
    uuid: safeString(row.ID?.toString()),

    region_id: regionId,
    district_id: districtId,
    council_id: councilId,
    ward_id: wardId,
    health_facility_id: healthFacilityId,
    registration_center_type_id: safeInteger(row.REGISTRATION_CENTER_TYPE_ID),

    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
  };
};

/**
 * Simple transformer without foreign keys (for reference tables)
 */
export const regionTransformer = (row: any) => {
  return {
    code: safeString(row.CODE),
    name: safeString(row.NAME),
    post_code: safeString(row.POST_CODE),
    intl_codes: safeString(row.INTL_CODES),
    active: safeBoolean(row.ACTIVE),
    uuid: safeString(row.ID?.toString()),
    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
  };
};
