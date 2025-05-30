SELECT DISTINCT
    CAST(A.PIN AS VARCHAR2(50)) AS provided_pin_no,
    B.OLD_REGISTRATION_NUMBER old_pin_no,
    B.NOTIFICATION_NUMBER notification_no,
    A.FIRST_NAME AS first_name,
    A.MIDDLE_NAME AS middle_name,
    A.LAST_NAME AS last_name,
    A.OTHER_NAMES other_name,
    A.NIN jamii_no,
    CAST(A.DATE_OF_BIRTH AS DATE) AS birth_date,
    CAST(H.CODE AS VARCHAR2(10)) AS sex_code,
    P.ID AS place_of_birth_id,
    P.DESCRIPTION AS place_of_birth_name,
    C.ID AS country_birth_id,
    C.NAME country_birth_name,
    B.HOSPITAL_ID facility_id,
    B.HOSPITAL_NAME facility_name,
    R.ID AS region_birth_id,
    R.NAME AS region_birth_name,
    D.ID AS district_birth_id,
    W.ID AS ward_birth_id,
    W.NAME AS ward_birth_name,
    D.NAME AS district_birth_name,
    CAST(B.REGISTRATION_DATE AS DATE) AS registration_date,
    '' AS registrar_name,
    F.FIRST_NAME AS mother_first_name,
    F.MIDDLE_NAME AS mother_middle_name,
    F.LAST_NAME AS mother_sur_name,
    F.OTHER_NAMES AS mother_other_name,
    F.COUNTRY_OF_BIRTH_ID AS mother_birth_country_id,
    C2.NAME mother_birth_country_name,
    F.NIN mother_jamii_no,
    G.FIRST_NAME AS father_first_name,
    G.MIDDLE_NAME AS father_middle_name,
    G.LAST_NAME AS father_sur_name,
    G.OTHER_NAMES AS mother_other_name,
    G.COUNTRY_OF_BIRTH_ID AS father_birth_country_id,
    C3.NAME father_birth_country_name,
    G.NIN father_jamii_no,
    B.CREATED_DATE created_date,
    '' photo_ref_no,
    '' is_migrated_data,
    '' migrated_date,
    B.MODIFIED_BY last_updated_by_id,
    B.MODIFIED_DATE last_updated_date,
    '' last_updated_by_email,
    '' certificate_status_id,
    '' last_track_date,
    NVL(A.FIRST_NAME, '') || ' ' ||
    NVL(A.MIDDLE_NAME, '') || ' ' ||
    NVL(A.LAST_NAME, '') AS child_full_name,
    NVL(F.FIRST_NAME, '') || ' ' ||
    NVL(F.MIDDLE_NAME, '') || ' ' ||
    NVL(F.LAST_NAME, '') AS mother_full_name,
    NVL(G.FIRST_NAME, '') || ' ' ||
    NVL(G.MIDDLE_NAME, '') || ' ' ||
    NVL(G.LAST_NAME, '') AS father_full_name,
    B.CERTIFICATE_PRINT_DATE first_printed_date,
    '' printed_count,
    B.CERTIFICATE_PRINT_DATE last_printed_date,
    '' attachment
FROM CRVS.PERSON A
JOIN CRVS.BIRTH_REGISTRATION B ON A.ID = B.CHILD_ID
LEFT JOIN CRVS.PERSON F ON F.ID = B.MOTHER_ID
LEFT JOIN CRVS.PERSON G ON G.ID = B.FATHER_ID
JOIN CRVS.SEX H ON A.SEX_ID = H.ID
JOIN CRVS.PLACE_OF_BIRTH P ON P.ID = B.PLACE_OF_BIRTH_ID
JOIN CRVS.REGION R ON R.ID = A.REGION_OF_BIRTH_ID
JOIN CRVS.DISTRICT D ON D.ID = A.DISTRICT_OF_BIRTH_ID
JOIN CRVS.WARD W ON W.ID = A.WARD_OF_BIRTH_ID
JOIN CRVS.COUNTRY C ON A.COUNTRY_OF_BIRTH_ID = C.ID
JOIN CRVS.COUNTRY C2 ON F.COUNTRY_OF_BIRTH_ID = C2.ID
JOIN CRVS.COUNTRY C3 ON G.COUNTRY_OF_BIRTH_ID = C3.ID
WHERE TRUNC(B.CREATED_DATE) >= TO_DATE('01-JANUARY-2025', 'DD-MON-YYYY')
