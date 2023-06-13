CREATE TABLE rus2000 (
  Date_YYYYMMDD DATE COMMENT '取得年月日',
  No INT,
  Symbol VARCHAR(255) COMMENT 'ティッカーシンボル',
  StockName VARCHAR(255) COMMENT '証券名称',
  Sector VARCHAR(255) COMMENT 'セクター',
  Industry VARCHAR(255) COMMENT '産業',
  INS_DATE DATETIME COMMENT 'データ登録日時',
  UPD_DATE DATETIME COMMENT 'データ更新日時',
  PRIMARY KEY (Date_YYYYMMDD, Symbol)
);
