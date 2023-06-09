CREATE TABLE industry (
  industry_id INT PRIMARY KEY AUTO_INCREMENT, -- 業種ID
  en_id INT NOT NULL, -- 英語ID
  INS_DATE datetime, -- 登録日時
  UPD_DATE datetime, -- 更新日時
  FOREIGN KEY (en_id) REFERENCES en_words (en_id), -- 英語IDの外部キー制約
  UNIQUE (en_id) -- en_idの重複を許容しない制約
);
