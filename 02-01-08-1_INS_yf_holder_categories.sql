INSERT INTO yf_holder_categories (
    category_name,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
  category_name = category_name, -- 更新しない
  INS_DATE = INS_DATE, -- 更新しない
  UPD_DATE = VALUES(UPD_DATE); -- UPD_DATEを新しい値で更新
