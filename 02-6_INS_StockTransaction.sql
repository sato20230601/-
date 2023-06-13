INSERT INTO StockTransaction (
    symbol,
    transaction_date,
    transaction_type,
    market,
    settlement_currency,
    exchange_rate,
    unit_price,
    quantity,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
