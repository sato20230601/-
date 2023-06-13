CREATE TABLE Trading_History (
  id INT AUTO_INCREMENT PRIMARY KEY,
  trade_date DATE, -- 約定日
  settlement_date DATE, -- 受渡日
  ticker VARCHAR(10), -- ティッカー
  security_name VARCHAR(255), -- 銘柄名
  account VARCHAR(255), -- 口座
  transaction_type VARCHAR(10), -- 取引区分
  buy_sell_type VARCHAR(10), -- 売買区分
  credit_type VARCHAR(10), -- 信用区分
  settlement_deadline VARCHAR(10), -- 弁済期限
  settlement_currency VARCHAR(10), -- 決済通貨
  quantity INT, -- 数量［株］
  unit_price DECIMAL(10, 4), -- 単価［USドル］
  execution_price DECIMAL(10, 2), -- 約定代金［USドル］
  exchange_rate DECIMAL(10, 2), -- 為替レート
  commission DECIMAL(10, 2), -- 手数料［USドル］
  tax DECIMAL(10, 2), -- 税金［USドル］
  settlement_amount_usd DECIMAL(10, 2), -- 受渡金額［USドル］
  settlement_amount_jpy DECIMAL(10, 2), -- 受渡金額［円］
  INS_DATE datetime,  -- 'データ登録日時'
  UPD_DATE datetime   -- 'データ更新日時'
);
