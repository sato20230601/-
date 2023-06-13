INSERT INTO Trading_History (
  trade_date, 
  settlement_date, 
  ticker, 
  security_name, 
  account, 
  transaction_type, 
  buy_sell_type, 
  credit_type, 
  settlement_deadline, 
  settlement_currency,
  quantity, 
  unit_price, 
  execution_price, 
  exchange_rate, 
  commission, 
  tax, 
  settlement_amount_usd, 
  settlement_amount_jpy, 
  INS_DATE,
  UPD_DATE
) VALUES ( 
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);

