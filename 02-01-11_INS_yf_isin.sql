INSERT INTO yf_isin (
    symbol,
    ISIN,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    ISIN = VALUES(ISIN),
    UPD_DATE = VALUES(UPD_DATE);
