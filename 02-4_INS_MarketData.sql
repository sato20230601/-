INSERT INTO MarketData (
    symbol,
    previousClose,
    open,
    dayLow,
    dayHigh,
    regularMarketPreviousClose,
    regularMarketOpen,
    regularMarketDayLow,
    regularMarketDayHigh,
    volume,
    regularMarketVolume,
    averageVolume,
    averageVolume10days,
    averageDailyVolume10Day,
    bid,
    ask,
    bidSize,
    askSize,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);