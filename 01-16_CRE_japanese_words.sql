-- 日本語の情報
CREATE TABLE japanese_words (
  jp_id INT PRIMARY KEY AUTO_INCREMENT, -- 日本語ID
  japanese VARCHAR(255) NOT NULL, -- 日本語のテキスト
  INS_DATE datetime, -- 登録日時
  UPD_DATE datetime -- 更新日時
);
