INSERT INTO yf_actions (
    ActDate,
    symbol,
    Dividends,
    StockSplits,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    Dividends = VALUES(Dividends),
    StockSplits = VALUES(StockSplits),
    UPD_DATE = VALUES(UPD_DATE);
