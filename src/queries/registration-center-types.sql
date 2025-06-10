-- Query to fetch registration center types from Oracle
SELECT 
    rct.ID,
    rct.CODE,
    rct.DESCRIPTION
FROM CRVS.REGISTRATION_CENTER_TYPE rct
ORDER BY rct.CODE
