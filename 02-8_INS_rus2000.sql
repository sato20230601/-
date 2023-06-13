INSERT INTO rus2000 (
    Date_YYYYMMDD,
    No,
    Symbol,
    StockName,
    Sector,
    Industry,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    No = VALUES(No),
    Symbol = VALUES(Symbol),
    StockName = VALUES(StockName),
    Sector = VALUES(Sector),
    Industry = VALUES(Industry),
    UPD_DATE = VALUES(UPD_DATE);
