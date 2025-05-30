/**
 * Field mappings for different tables
 * Maps Oracle column names (keys) to PostgreSQL column names (values)
 */

export const birthRegistrationMapping = {
  // Child Information
  PROVIDED_PIN_NO: "provided_pin_no",
  OLD_PIN_NO: "old_pin_no",
  NOTIFICATION_NO: "notification_no",
  FIRST_NAME: "first_name",
  MIDDLE_NAME: "middle_name",
  LAST_NAME: "last_name",
  OTHER_NAME: "other_name",
  JAMII_NO: "jamii_no",
  BIRTH_DATE: "birth_date",
  SEX_CODE: "sex_code",

  // Place of Birth
  PLACE_OF_BIRTH_ID: "place_of_birth_id",
  PLACE_OF_BIRTH_NAME: "place_of_birth_name",
  COUNTRY_BIRTH_ID: "country_birth_id",
  COUNTRY_BIRTH_NAME: "country_birth_name",

  // Facility Information
  FACILITY_ID: "facility_id",
  FACILITY_NAME: "facility_name",

  // Location Information
  REGION_BIRTH_ID: "region_birth_id",
  REGION_BIRTH_NAME: "region_birth_name",
  DISTRICT_BIRTH_ID: "district_birth_id",
  DISTRICT_BIRTH_NAME: "district_birth_name",
  WARD_BIRTH_ID: "ward_birth_id",
  WARD_BIRTH_NAME: "ward_birth_name",

  // Registration Information
  REGISTRATION_DATE: "registration_date",
  REGISTRAR_NAME: "registrar_name",

  // Mother Information
  MOTHER_FIRST_NAME: "mother_first_name",
  MOTHER_MIDDLE_NAME: "mother_middle_name",
  MOTHER_SUR_NAME: "mother_sur_name",
  MOTHER_OTHER_NAME: "mother_other_name",
  MOTHER_BIRTH_COUNTRY_ID: "mother_birth_country_id",
  MOTHER_BIRTH_COUNTRY_NAME: "mother_birth_country_name",
  MOTHER_JAMII_NO: "mother_jamii_no",

  // Father Information
  FATHER_FIRST_NAME: "father_first_name",
  FATHER_MIDDLE_NAME: "father_middle_name",
  FATHER_SUR_NAME: "father_sur_name",
  FATHER_OTHER_NAME: "father_other_name",
  FATHER_BIRTH_COUNTRY_ID: "father_birth_country_id",
  FATHER_BIRTH_COUNTRY_NAME: "father_birth_country_name",
  FATHER_JAMII_NO: "father_jamii_no",

  // System Information - NOTE: Skip birth_certificate_id as it's auto-generated
  CREATED_DATE: "created_date",
  PHOTO_REF_NO: "photo_ref_no",
  IS_MIGRATED_DATA: "is_migrated_data",
  MIGRATED_DATE: "migrated_date",
  LAST_UPDATED_BY_ID: "last_updated_by_id",
  LAST_UPDATED_DATE: "last_updated_date",
  LAST_UPDATED_BY_EMAIL: "last_updated_by_email",
  CERTIFICATE_STATUS_ID: "certificate_status_id",
  LAST_TRACK_DATE: "last_track_date",

  // Computed Full Names
  CHILD_FULL_NAME: "child_full_name",
  MOTHER_FULL_NAME: "mother_full_name",
  FATHER_FULL_NAME: "father_full_name",

  // Certificate Information
  FIRST_PRINTED_DATE: "first_printed_date",
  PRINTED_COUNT: "printed_count",
  LAST_PRINTED_DATE: "last_printed_date",
  ATTACHMENT: "attachment",
};
