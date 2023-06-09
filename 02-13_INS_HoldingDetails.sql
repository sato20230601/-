INSERT INTO HoldingDetails (
  Date_YYYYMMDD,
  Type,
  Ticker,
  SecurityName,
  Account,
  Quantity,
  QuantityUnit,
  AverageAcquisitionPrice,
  AcquisitionPriceUnit,
  CurrentPrice,
  CurrentPriceUnit,
  LastUpdateDate,
  ReferenceExchangeRate,
  DailyChange,
  DailyChangeUnit,
  MarketValueJPY,
  MarketValueForeign,
  EvaluationProfitLossJPY,
  EvaluationProfitLossPercentage,
  INS_DATE,
  UPD_DATE
) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
ON DUPLICATE KEY UPDATE
  Ticker = VALUES(Ticker),
  Account = VALUES(Account),
  Quantity = VALUES(Quantity),
  QuantityUnit = VALUES(QuantityUnit),
  AverageAcquisitionPrice = VALUES(AverageAcquisitionPrice),
  AcquisitionPriceUnit = VALUES(AcquisitionPriceUnit),
  CurrentPrice = VALUES(CurrentPrice),
  CurrentPriceUnit = VALUES(CurrentPriceUnit),
  LastUpdateDate = VALUES(LastUpdateDate),
  ReferenceExchangeRate = VALUES(ReferenceExchangeRate),
  DailyChange = VALUES(DailyChange),
  DailyChangeUnit = VALUES(DailyChangeUnit),
  MarketValueJPY = VALUES(MarketValueJPY),
  MarketValueForeign = VALUES(MarketValueForeign),
  EvaluationProfitLossJPY = VALUES(EvaluationProfitLossJPY),
  EvaluationProfitLossPercentage = VALUES(EvaluationProfitLossPercentage),
  UPD_DATE = VALUES(UPD_DATE);
