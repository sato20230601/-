-- 英語と日本語をそれぞれのIDで紐づける。その組み合わせを主キーとして一元管理する。
CREATE TABLE en_jp_translation (
  lan_tra_id INT PRIMARY KEY AUTO_INCREMENT, -- 言語翻訳ID
  en_id INT NOT NULL, -- 英語ID
  jp_id INT NOT NULL, -- 日本語ID
  INS_DATE datetime, -- 登録日時
  UPD_DATE datetime, -- 更新日時
  FOREIGN KEY (en_id) REFERENCES en_words (en_id), -- 英語IDの外部キー制約
  FOREIGN KEY (jp_id) REFERENCES jp_words (jp_id), -- 日本語IDの外部キー制約
  UNIQUE (en_id, jp_id) -- en_idとjp_idの組み合わせの重複を許容しない制約
);
