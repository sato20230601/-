INSERT INTO yf_shares_full (
    DateTime_Share,
    symbol,
    Shares,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    Shares = VALUES(Shares),
    UPD_DATE = VALUES(UPD_DATE);
