CREATE TABLE ExchangeRate (
  Date_YYYYMMDD DATE, -- 取得年月日
  Currency VARCHAR(20), -- 通貨
  Rate DECIMAL(18, 2), -- レート
  Pair VARCHAR(10), -- 通貨ペア
  LastUpdate DATETIME, -- 最終更新日時
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  PRIMARY KEY (Date_YYYYMMDD, Currency)
); -- 為替レート
