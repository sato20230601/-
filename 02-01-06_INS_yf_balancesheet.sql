INSERT INTO yf_balancesheet (
    Date_YYYYMMDD,
    symbol,
    OrdinarySharesNumber,
    ShareIssued,
    TotalDebt,
    TangibleBookValue,
    InvestedCapital,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    OrdinarySharesNumber = VALUES(OrdinarySharesNumber),
    ShareIssued = VALUES(ShareIssued),
    TotalDebt = VALUES(TotalDebt),
    TangibleBookValue = VALUES(TangibleBookValue),
    InvestedCapital = VALUES(InvestedCapital),
    UPD_DATE = VALUES(UPD_DATE);
