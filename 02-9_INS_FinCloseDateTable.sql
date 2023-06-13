INSERT INTO FinCloseDateTable (
    Symbol,
    FinCloseMon,
    FinCloseDate,
    market,
    status,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, '%s', %s, %s)
ON DUPLICATE KEY UPDATE
    FinCloseDate = VALUES(FinCloseDate),
    market = VALUES(market),
    status = VALUES(status),
    UPD_DATE = VALUES(UPD_DATE);
