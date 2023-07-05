-- 英語の情報
CREATE TABLE english_words (
  en_id INT PRIMARY KEY AUTO_INCREMENT, -- 英語ID
  english VARCHAR(255) NOT NULL, -- 英語のテキスト
  INS_DATE datetime, -- 登録日時
  UPD_DATE datetime -- 更新日時
);
