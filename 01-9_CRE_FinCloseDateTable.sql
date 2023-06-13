CREATE TABLE FinCloseDateTable (
  Symbol VARCHAR(255) COMMENT 'ティッカーシンボル',
  FinCloseMon VARCHAR(6) COMMENT '決算月:YYYYMM',
  FinCloseDate DATE COMMENT '決算日:YYYYMMDD',
  market VARCHAR(10) COMMENT '市場',
  status VARCHAR(256) COMMENT 'ステータス',
  INS_DATE DATETIME COMMENT 'データ登録日時',
  UPD_DATE DATETIME COMMENT 'データ更新日時',
  PRIMARY KEY (Symbol,FinCloseMon)
);
