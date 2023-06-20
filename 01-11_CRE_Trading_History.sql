CREATE TABLE Trading_History (
  trade_date DATE, -- 約定日 CSV01 Key01
  settlement_date DATE, -- 受渡日 CSV02 Key02
  ticker VARCHAR(10), -- ティッカー CSV03 Key03
  buy_sell_type VARCHAR(10), -- 売買区分 CSV07 Key04
  No INT, -- レコードNo Key05
  security_name VARCHAR(255), -- 銘柄名 CSV04
  account VARCHAR(255), -- 口座 CSV05
  transaction_type VARCHAR(10), -- 取引区分 CSV06
  credit_type VARCHAR(10), -- 信用区分 CSV08
  settlement_deadline VARCHAR(10), -- 弁済期限 CSV09
  settlement_currency VARCHAR(10), -- 決済通貨 CSV10
  quantity INT, -- 数量［株］CSV11
  unit_price DECIMAL(10, 4), -- 単価［USドル］CSV12
  execution_price DECIMAL(10, 2), -- 約定代金［USドル］CSV13
  exchange_rate DECIMAL(10, 2), -- 為替レート CSV14
  commission DECIMAL(10, 2), -- 手数料［USドル］CSV15
  tax DECIMAL(10, 2), -- 税金［USドル］CSV16
  settlement_amount_usd DECIMAL(10, 2), -- 受渡金額［USドル］CSV17
  settlement_amount_jpy DECIMAL(10, 2), -- 受渡金額［円］CSV18
  INS_DATE datetime,  -- 'データ登録日時'
  UPD_DATE datetime,  -- 'データ更新日時'
  PRIMARY KEY (trade_date, settlement_date, ticker, buy_sell_type, No)
)
