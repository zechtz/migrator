import { safeString, safeInteger, safeBoolean } from "../utils/data-helpers.js";
import { EnhancedTransformFunction } from "../types/index.js";

/**
 * Fixed district transformer - uses REGION_ID for direct assignment
 */
export const districtTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  // For districts, we can either use direct assignment or resolve region code
  // Option 1: Direct assignment if REGION_ID is already the correct FK
  const regionId = safeInteger(row.REGION_ID);
  // Option 2: If you need to resolve via code, use this instead:
  // const regionId = resolvers?.resolveRegionId
  //   ? await resolvers.resolveRegionId(row.REGION_CODE)
  //   : null;

  return {
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
};

/**
 * Council transformer - uses DISTRICT_CODE for resolution
 */
export const councilTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  const districtId = resolvers?.resolveDistrictId
    ? await resolvers.resolveDistrictId(row.DISTRICT_CODE) // Use DISTRICT_CODE from query
    : null;

  return {
    code: safeString(row.CODE),
    name: safeString(row.NAME),
    post_code: safeString(row.POST_CODE),
    mvc_uuid: safeString(row.MVC_UUID),
    active: safeBoolean(row.ACTIVE),
    uuid: safeString(row.ID?.toString()),
    district_id: districtId,
    district: safeString(row.DISTRICT_CODE),
    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
  };
};

/**
 * Ward transformer - uses COUNCIL_CODE for resolution
 */
export const wardTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  const councilId = resolvers?.resolveCouncilId
    ? await resolvers.resolveCouncilId(row.COUNCIL_CODE) // Use COUNCIL_CODE from query
    : null;

  const districtId = resolvers?.resolveDistrictId
    ? await resolvers.resolveDistrictId(row.DISTRICT_CODE) // Use DISTRICT_CODE from query
    : null;

  return {
    ward_id: safeInteger(row.ID),
    code: safeString(row.CODE),
    ward_name: safeString(row.NAME),
    post_code: safeString(row.POST_CODE),
    is_active: safeBoolean(row.ACTIVE),
    active: safeBoolean(row.ACTIVE),
    uuid: safeString(row.ID?.toString()),
    council_id: councilId,
    district: districtId,
    council: safeString(row.COUNCIL_CODE),
    council_name: safeString(row.COUNCIL_CODE),
    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
  };
};

/**
 * Health facility transformer - uses COUNCIL_CODE for resolution
 */
export const healthFacilityTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  const councilId = resolvers?.resolveCouncilId
    ? await resolvers.resolveCouncilId(row.COUNCIL_CODE) // Use COUNCIL_CODE from query
    : null;

  return {
    health_facility_id: safeInteger(row.ID),
    code: safeString(row.CODE),
    name: safeString(row.NAME),
    post_code: safeString(row.POST_CODE),
    active: safeBoolean(row.ACTIVE),
    uuid: safeString(row.ID?.toString()),
    council_id: councilId,
    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
  };
};

/**
 * Registration center transformer - handles multiple foreign keys
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

    // Foreign key assignments
    region_id: regionId,
    district_id: districtId,
    council_id: councilId,
    ward_id: safeInteger(wardId), // Convert to integer for ward_id column
    health_facility_id: healthFacilityId,
    registration_center_type_id: safeInteger(row.REGISTRATION_CENTER_TYPE_ID),

    // Audit fields
    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
    activated_at: new Date(), // Set to current time as default
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
