INSERT INTO AssetData (
  Date_YYYYMMDD,
  Category,
  MarketValueJPY,
  DailyChangeJPY,
  DailyChangePercentage,
  MonthlyChangeJPY,
  MonthlyChangePercentage,
  EvaluationProfitLossJPY,
  EvaluationProfitLossPercentage,
  RealizedProfitLossJPY,
  DividendCurrencyJPY,
  DividendCurrencyForeign,
  INS_DATE,
  UPD_DATE
)
VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
ON DUPLICATE KEY UPDATE
  MarketValueJPY = VALUES(MarketValueJPY),
  DailyChangeJPY = VALUES(DailyChangeJPY),
  DailyChangePercentage = VALUES(DailyChangePercentage),
  MonthlyChangeJPY = VALUES(MonthlyChangeJPY),
  MonthlyChangePercentage = VALUES(MonthlyChangePercentage),
  EvaluationProfitLossJPY = VALUES(EvaluationProfitLossJPY),
  EvaluationProfitLossPercentage = VALUES(EvaluationProfitLossPercentage),
  RealizedProfitLossJPY = VALUES(RealizedProfitLossJPY),
  DividendCurrencyJPY = VALUES(DividendCurrencyJPY),
  DividendCurrencyForeign = VALUES(DividendCurrencyForeign),
  UPD_DATE = VALUES(UPD_DATE);
