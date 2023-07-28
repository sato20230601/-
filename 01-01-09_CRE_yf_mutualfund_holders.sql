-- ファンドの保有者データ
CREATE TABLE yf_mutualfund_holders (
  Date_YYYYMMDD DATE,
  symbol VARCHAR(255),
  category_id INT,
  Value BIGINT,
  INS_DATE DATETIME,
  UPD_DATE DATETIME,
  PRIMARY KEY (Date_YYYYMMDD, symbol, category_id),
  FOREIGN KEY (category_id) REFERENCES yf_holder_categories(id)
);
