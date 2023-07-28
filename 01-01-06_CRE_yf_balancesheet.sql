-- 銘柄の貸借対照表データ
CREATE TABLE yf_balancesheet (
  Date_YYYYMMDD DATE, -- 日付 (YYYY-MM-DD)
  symbol VARCHAR(255), -- 銘柄コード
  OrdinarySharesNumber FLOAT, -- 普通株式数
  ShareIssued FLOAT, -- 発行済株式数
  TotalDebt FLOAT, -- 総負債
  TangibleBookValue FLOAT, -- 有形純資産
  InvestedCapital FLOAT, -- 投資資本
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  PRIMARY KEY (Date_YYYYMMDD, symbol)
);
