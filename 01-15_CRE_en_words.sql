-- 英語の情報
CREATE TABLE en_words (
  en_id INT PRIMARY KEY AUTO_INCREMENT, -- 英語ID
  english VARCHAR(255) NOT NULL, -- 英語のテキスト
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  UNIQUE (english) -- englishカラムの値の重複を許容しない
);
