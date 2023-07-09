INSERT INTO sector (
  en_id,
  INS_DATE,
  UPD_DATE
) 
VALUES ( %s, %s, %s) 
ON DUPLICATE KEY UPDATE
  UPD_DATE = VALUES(UPD_DATE);
