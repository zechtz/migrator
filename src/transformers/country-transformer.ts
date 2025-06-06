import { safeString, safeInteger } from "../utils/data-helpers.js";

export const countryTransformer = (row: any) => {
  return {
    country_code: safeString(row.CODE, 3),
    country_name_en: safeString(row.NAME, 100),
    country_name_fr: safeString(row.NAME, 100),
    nationality_en: null,
    nationality_fr: null,
    translate_code: safeString(row.INTL_CODE, 10),
    is_active: true,
    online_id: safeInteger(row.ID),
  };
};
