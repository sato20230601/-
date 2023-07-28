-- インサイダーや機関投資家別の保有割合と保有者数
CREATE TABLE yf_major_holders (
  Date_YYYYMMDD DATE,
  symbol VARCHAR(255),
  category_id INT,
  Value VARCHAR(255),
  INS_DATE DATETIME,
  UPD_DATE DATETIME,
  PRIMARY KEY (Date_YYYYMMDD, symbol, category_id),
  FOREIGN KEY (category_id) REFERENCES yf_holder_categories(id)
);
