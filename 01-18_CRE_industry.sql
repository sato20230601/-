CREATE TABLE industry (
  industry_id INT PRIMARY KEY AUTO_INCREMENT, -- 業種ID
  en_id INT NOT NULL, -- 英語ID
  overview_en TEXT, -- 英語の概要
  INS_DATE datetime, -- 登録日時
  UPD_DATE datetime, -- 更新日時
  FOREIGN KEY (en_id) REFERENCES english_words (en_id) -- 英語IDの外部キー制約
);
