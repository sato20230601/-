INSERT INTO sec_ind_symbol (
  sec_ind_id,
  symbol,
  INS_DATE,
  UPD_DATE
)
VALUES ( %s, %s, %s, %s) 
ON DUPLICATE KEY UPDATE
  UPD_DATE = VALUES(UPD_DATE);
