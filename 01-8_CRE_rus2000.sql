CREATE TABLE rus2000 (
  Date_YYYYMMDD DATE, -- '取得年月日'
  Symbol VARCHAR(255), -- 'ティッカーシンボル'
  StockName VARCHAR(255), -- '証券名称'
  Sector VARCHAR(255), -- 'セクター'
  Industry VARCHAR(255), -- '産業'
  INS_DATE DATETIME, -- 'データ登録日時'
  UPD_DATE DATETIME, -- 'データ更新日時'
  PRIMARY KEY (Date_YYYYMMDD, Symbol)
);
