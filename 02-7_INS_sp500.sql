INSERT INTO sp500 (
    Date_YYYYMMDD,
    Symbol,
    Security_Name,
    GICS_Sector,
    GICS_Sub_Industry,
    Headquarters_Location,
    Date_added,
    CIK,
    Founded,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    Symbol = VALUES(Symbol),
    Security_Name = VALUES(Security_Name),
    GICS_Sector = VALUES(GICS_Sector),
    GICS_Sub_Industry = VALUES(GICS_Sub_Industry),
    Headquarters_Location = VALUES(Headquarters_Location),
    Date_added = VALUES(Date_added),
    CIK = VALUES(CIK),
    UPD_DATE = VALUES(UPD_DATE);
