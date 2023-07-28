-- isin情報
CREATE TABLE yf_isin (
  symbol VARCHAR(255), -- 銘柄コード
  ISIN VARCHAR(255), -- ISINコード
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  PRIMARY KEY (symbol)
);
