CREATE TABLE HoldingDetails (
  Date_YYYYMMDD DATE, -- 取得年月日
  Type VARCHAR(50), -- 種別
  Ticker VARCHAR(10), -- 銘柄コード・ティッカー
  SecurityName VARCHAR(100), -- 銘柄
  Account VARCHAR(50), -- 口座
  Quantity INT, -- 保有数量
  QuantityUnit VARCHAR(10), -- ［単位］
  AverageAcquisitionPrice DECIMAL(18, 2), -- 平均取得価額
  AcquisitionPriceUnit VARCHAR(10), -- ［単位］
  CurrentPrice DECIMAL(18, 2), -- 現在値
  CurrentPriceUnit VARCHAR(10), -- ［単位］
  LastUpdateDate DATE, -- 現在値(更新日)
  ReferenceExchangeRate DECIMAL(18, 2), -- (参考為替)
  DailyChange DECIMAL(18, 2), -- 前日比
  DailyChangeUnit VARCHAR(10), -- ［単位］
  MarketValueJPY VARCHAR(18), -- 時価評価額[円]
  MarketValueForeign VARCHAR(30), -- 時価評価額[外貨]
  EvaluationProfitLossJPY VARCHAR(18), -- 評価損益[円]
  EvaluationProfitLossPercentage DECIMAL(18, 2), -- 評価損益[％]
  INS_DATE datetime,
  UPD_DATE datetime,
  PRIMARY KEY (Date_YYYYMMDD, Type, SecurityName)
); -- 保有商品詳細
