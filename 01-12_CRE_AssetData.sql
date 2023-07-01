CREATE TABLE AssetData (
  Date_YYYYMMDD DATE, -- 取得年月日
  Category VARCHAR(22), -- 資産のカテゴリを示す列
  MarketValueJPY VARCHAR(18), -- 時価評価額[円]（単位は円）
  DailyChangeJPY VARCHAR(18), -- 前日比[円]（単位は円）
  DailyChangePercentage VARCHAR(8), -- 前日比[％]（単位は％）
  MonthlyChangeJPY VARCHAR(18), -- 前月比[円]（単位は円）
  MonthlyChangePercentage VARCHAR(18), -- 前月比[％]（単位は％）
  EvaluationProfitLossJPY VARCHAR(18), -- 評価損益[円]（単位は円）
  EvaluationProfitLossPercentage VARCHAR(8), -- 評価損益[％]（単位は％）
  RealizedProfitLossJPY VARCHAR(18), -- 実現損益[円]（単位は円）
  DividendCurrencyJPY VARCHAR(18), -- 配当・分配金[円貨]（単位は円貨）
  DividendCurrencyForeign VARCHAR(18), -- 配当・分配金[外貨]（単位は外貨）
  INS_DATE datetime,
  UPD_DATE datetime,
  PRIMARY KEY (Date_YYYYMMDD, Category)
); -- 資産データ
