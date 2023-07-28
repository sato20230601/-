-- インサイダーや機関投資家などのカテゴリ
CREATE TABLE yf_holder_categories (
  id INT PRIMARY KEY AUTO_INCREMENT,
  category_name VARCHAR(255) UNIQUE,
  INS_DATE DATETIME,
  UPD_DATE DATETIME
);
