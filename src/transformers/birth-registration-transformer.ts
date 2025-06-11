import {
  safeString,
  safeInteger,
  safeDate,
  safeIntegerWithDefault,
  safeDateWithDefault,
} from "../utils/data-helpers.js";

import { TransformFunction } from "../types/index.js";

function buildFullName(
  firstName?: string,
  middleName?: string,
  lastName?: string,
): string {
  const parts = [firstName, middleName, lastName].filter(
    (part) => part && part.trim() !== "" && part.toLowerCase() !== "null",
  );
  return parts.length > 0 ? parts.join(" ") : "";
}

/**
 * Birth registration migration configuration with enhanced FK resolution
 */
export const birthRegistrationTransformer: TransformFunction = (row: any) => {
  return {
    provided_pin_no: safeString(row.PROVIDED_PIN_NO, 20),
    old_pin_no: safeString(row.OLD_PIN_NO, 20),
    notification_no: safeString(row.NOTIFICATION_NUMBER?.toString(), 20),
    first_name: safeString(row.FIRST_NAME, 50),
    middle_name: safeString(row.MIDDLE_NAME, 50),
    last_name: safeString(row.LAST_NAME, 50),
    other_name: safeString(row.OTHER_NAME, 100),
    jamii_no: safeString(row.JAMII_NO?.toString(), 50),
    birth_date: safeDate(row.BIRTH_DATE),
    sex_code: safeString(row.SEX_CODE, 10),

    // Location Information (Oracle IDs - may need mapping later)
    region_birth_id: safeInteger(row.REGION_BIRTH_ID),
    district_birth_id: safeInteger(row.DISTRICT_BIRTH_ID),
    ward_birth_id: safeInteger(row.WARD_BIRTH_ID),
    country_birth_id: safeInteger(row.COUNTRY_BIRTH_ID),

    region_birth_name: safeString(row.REGION_BIRTH_NAME) || "Unknown Region",
    district_birth_name:
      safeString(row.DISTRICT_BIRTH_NAME) || "Unknown District",
    ward_birth_name: safeString(row.WARD_BIRTH_NAME) || "Unknown Ward",
    country_birth_name: safeString(row.COUNTRY_BIRTH_NAME) || "Tanzania",

    // Place of Birth
    place_of_birth_id: safeInteger(row.PLACE_OF_BIRTH_ID),
    place_of_birth_name: safeString(row.PLACE_OF_BIRTH_NAME, 10) || "Hospital",

    // Facility Information
    facility_id: safeInteger(row.FACILITY_ID),
    facility_name: safeString(row.FACILITY_NAME, 100),

    // Registration Information
    registration_date: safeDate(row.REGISTRATION_DATE),
    registrar_name: safeString(row.REGISTRAR_NAME) || "System Migrated",

    // Parent Information (set to null for now - can be populated in separate migration)
    mother_first_name: safeString(row.MOTHER_FIRST_NAME),
    mother_middle_name: safeString(row.MOTHER_MIDDLE_NAME),
    mother_sur_name: safeString(row.MOTHER_SUR_NAME),
    mother_other_name: safeString(row.MOTHER_OTHER_NAME),
    mother_birth_country_id: safeInteger(row.MOTHER_BIRTH_COUNTRY_ID),
    mother_birth_country_name: safeString(row.MOTHER_BIRTH_COUNTRY_NAME),
    mother_jamii_no: safeString(row.MOTHER_JAMII_NO),

    father_first_name: safeString(row.FATHER_FIRST_NAME),
    father_middle_name: safeString(row.FATHER_MIDDLE_NAME),
    father_sur_name: safeString(row.FATHER_SUR_NAME),
    father_other_name: safeString(row.FATHER_OTHER_NAME),
    father_birth_country_id: safeInteger(row.FATHER_BIRTH_COUNTRY_ID),
    father_birth_country_name: safeString(row.FATHER_BIRTH_COUNTRY_NAME),
    father_jamii_no: safeString(row.FATHER_JAMII_NO),

    // System Information
    created_date: safeDate(row.CREATED_DATE),
    photo_ref_no: safeString(row.PHOTO_REF_NO),
    is_migrated_data: true, // Mark as migrated
    migrated_date: new Date(),
    last_updated_by_id: safeInteger(row.LAST_UPDATED_BY_ID),
    last_updated_date: safeDate(row.LAST_UPDATED_DATE),
    last_updated_by_email: safeString(row.LAST_UPDATED_BY_EMAIL),

    // Required fields with defaults
    certificate_status_id: safeIntegerWithDefault(1)(row.CERTIFICATE_STATUS_ID),
    last_track_date: safeDateWithDefault(new Date())(row.LAST_TRACK_DATE),
    printed_count: safeIntegerWithDefault(0)(row.PRINTED_COUNT),

    // Computed Full Names
    child_full_name: buildFullName(
      row.FIRST_NAME,
      row.MIDDLE_NAME,
      row.LAST_NAME,
    ),
    mother_full_name: buildFullName(
      row.MOTHER_FIRST_NAME,
      row.MOTHER_MIDDLE_NAME,
      row.MOTHER_SUR_NAME,
    ),
    father_full_name: buildFullName(
      row.FATHER_FIRST_NAME,
      row.FATHER_MIDDLE_NAME,
      row.FATHER_SUR_NAME,
    ),

    // Certificate Information
    first_printed_date: safeDate(row.FIRST_PRINTED_DATE),
    last_printed_date: safeDate(row.FIRST_PRINTED_DATE),
    attachment: null,
  };
};
