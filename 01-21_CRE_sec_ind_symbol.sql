-- 企業情報に存在する「Symbol」に対して「セクターと業種」の組み合わせで一元管理する。
CREATE TABLE sec_ind_symbol (
  sec_ind_id INT,
  symbol VARCHAR(255),
  INS_DATE datetime, -- 登録日時
  UPD_DATE datetime, -- 更新日時
  PRIMARY KEY (sec_ind_id, symbol),
  FOREIGN KEY (sec_ind_id) REFERENCES sec_ind (sec_ind_id) -- セクター業種IDの外部キー制約
);
