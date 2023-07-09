-- 日本語の情報
CREATE TABLE jp_words (
  jp_id INT PRIMARY KEY AUTO_INCREMENT, -- 日本語ID
  japanese VARCHAR(255) NOT NULL, -- 日本語のテキスト
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  UNIQUE (japanese) -- japaneseカラムの値の重複を許容しない
);
