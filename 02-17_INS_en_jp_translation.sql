INSERT INTO en_jp_translation (
  en_id,
  jp_id,
  INS_DATE,
  UPD_DATE
)VALUES ( %s, %s, %s, %s) 
ON DUPLICATE KEY UPDATE
  UPD_DATE = VALUES(UPD_DATE);
