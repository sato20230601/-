-- 銘柄の株式数データ
CREATE TABLE yf_shares_full (
  DateTime_Share DATETIME, -- 日付と時刻 (YYYY-MM-DD HH:MM:SS)
  symbol VARCHAR(255), -- 銘柄コード
  Shares BIGINT, -- 株式数
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  PRIMARY KEY (DateTime_Share, symbol)
);
