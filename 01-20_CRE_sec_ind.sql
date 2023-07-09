-- 企業情報に存在する「Symbol」に対して「セクターと業種」の組み合わせで一元管理する。
CREATE TABLE sec_ind (
  sec_ind_id INT PRIMARY KEY AUTO_INCREMENT, -- セクター業種ID
  sector_id INT NOT NULL, -- セクターID
  industry_id INT NOT NULL, -- 業種ID
  INS_DATE datetime, -- 登録日時
  UPD_DATE datetime, -- 更新日時
  FOREIGN KEY (sector_id) REFERENCES sector (sector_id), -- セクターIDの外部キー制約
  FOREIGN KEY (industry_id) REFERENCES industry (industry_id), -- 業種IDの外部キー制約
  UNIQUE (sector_id, industry_id) -- sector_idとindustry_idの組み合わせの重複を許容しない制約
);
