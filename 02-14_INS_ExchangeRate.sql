INSERT INTO ExchangeRate (
  Date_YYYYMMDD,
  Currency,
  Rate,
  Pair,
  LastUpdate,
  INS_DATE,
  UPD_DATE
) 
VALUES ( %s, %s, %s, %s, %s, %s, %s) 
ON DUPLICATE KEY UPDATE
  Rate = VALUES(Rate),
  Pair = VALUES(Pair),
  LastUpdate = VALUES(LastUpdate),
  UPD_DATE = VALUES(UPD_DATE);
