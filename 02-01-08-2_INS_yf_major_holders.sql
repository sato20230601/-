INSERT INTO yf_major_holders (
    Date_YYYYMMDD,
    symbol,
    category_id,
    Value,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    Value = VALUES(Value),
    UPD_DATE = VALUES(UPD_DATE);
