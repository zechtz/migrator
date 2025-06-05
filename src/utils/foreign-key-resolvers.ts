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
