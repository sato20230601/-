INSERT INTO yf_mutualfund_holders (
    Date_YYYYMMDD,
    symbol,
    category_id,
    Value,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    Shares = VALUES(Shares),
    Value = VALUES(Value),
    UPD_DATE = VALUES(UPD_DATE);
