import { safeString, safeInteger, safeBoolean } from "../utils/data-helpers.js";
import { EnhancedTransformFunction } from "../types/index.js";

export const districtTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  const regionId = safeInteger(row.REGION_ID);

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
 * Registration Center Type transformer (reference table - no FKs needed)
 */
export const registrationCenterTypeTransformer = (row: any) => {
  return {
    id: safeInteger(row.ID),
    code: safeString(row.CODE),
    description: safeString(row.DESCRIPTION),
    created_at: new Date(),
    updated_at: new Date(),
    deleted: false,
  };
};

/**
 * Health facility transformer - matches actual PostgreSQL table schema
 */
export const healthFacilityTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  const councilId = resolvers?.resolveCouncilId
    ? await resolvers.resolveCouncilId(row.COUNCIL_CODE)
    : null;

  return {
    health_facility_name: safeString(row.NAME),
    health_facility_code: safeString(row.CODE),
    health_facility_type_id: null,
    is_active: safeBoolean(row.ACTIVE),
    created_date: new Date(),
    created_by_id: null,
    updated_date: new Date(),
    updated_by_id: null,

    active: row.ACTIVE === 1 ? "Y" : "N",
    code: safeString(row.CODE),
    created_at: new Date(),
    created_by: null,
    deactivated_at: null,
    deactivated_by: null,
    deleted: false,
    deleted_at: null,
    name: safeString(row.NAME),
    post_code: safeInteger(row.POST_CODE),
    updated_at: new Date(),
    updated_by: null,
    uuid: safeString(row.ID?.toString()),

    // Foreign keys
    council_id: councilId,
    council_hierarchy_id: null,
  };
};

/**
 * Updated Registration center transformer - uses correct field names
 */
export const registrationCenterTransformer: EnhancedTransformFunction = async (
  row,
  resolvers,
) => {
  // Resolve multiple foreign keys using the correct field names from your query
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
    ? await resolvers.resolveHealthFacilityId(row.HEALTHFAC_CODE)
    : null;

  const registrationCenterTypeId = resolvers?.resolveRegistrationCenterTypeId
    ? resolvers.resolveRegistrationCenterTypeId(row.HEALTH_FACILITY_CODE)
    : null;

  return {
    mgt_registration_center_id: safeInteger(row.ID),
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
    registration_center_type_id: registrationCenterTypeId,

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
