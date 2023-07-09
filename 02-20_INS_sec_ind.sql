INSERT INTO sec_ind (
  sector_id,
  industry_id,
  INS_DATE,
  UPD_DATE
)
VALUES ( %s, %s, %s, %s) 
ON DUPLICATE KEY UPDATE
  UPD_DATE = VALUES(UPD_DATE);
