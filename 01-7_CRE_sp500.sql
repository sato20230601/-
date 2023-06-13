CREATE TABLE sp500 (
  Date_YYYYMMDD DATE COMMENT '取得年月日',
  Symbol VARCHAR(255) COMMENT 'ティッカーシンボル',
  Security_Name VARCHAR(255) COMMENT '証券名称',
  GICS_Sector VARCHAR(255) COMMENT 'GICSセクター',
  GICS_Sub_Industry VARCHAR(255) COMMENT 'GICSサブ業種',
  Headquarters_Location VARCHAR(255) COMMENT '本社所在地',
  Date_added VARCHAR(255) COMMENT '追加年月日',
  CIK VARCHAR(255) COMMENT 'CIK',
  Founded VARCHAR(255) COMMENT '設立年',
  INS_DATE DATETIME COMMENT 'データ登録日時',
  UPD_DATE DATETIME COMMENT 'データ更新日時',
  PRIMARY KEY (Date_YYYYMMDD, Symbol)
);
