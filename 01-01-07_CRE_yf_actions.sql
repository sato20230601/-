-- 銘柄のアクションデータ (actions)
CREATE TABLE yf_actions (
  ActDate DATE, -- 日付 (YYYY-MM-DD)
  symbol VARCHAR(255), -- 銘柄コード
  Dividends FLOAT,-- 配当
  StockSplits FLOAT,-- 株式分割
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  PRIMARY KEY (ActDate, symbol)
);
