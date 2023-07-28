-- ニュースデータ
CREATE TABLE yf_news (
  Date_YYYYMMDD DATE, -- 日付 (YYYY-MM-DD)
  symbol VARCHAR(255), -- 銘柄コード
  uuid VARCHAR(255), -- ユニークな識別子
  title VARCHAR(255), -- タイトル
  publisher VARCHAR(255), -- パブリッシャー
  link VARCHAR(255), -- リンク
  providerPublishTime INT, -- プロバイダの公開日時
  type VARCHAR(255), -- タイプ
  relatedTickers VARCHAR(255), -- 関連する銘柄
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  PRIMARY KEY (Date_YYYYMMDD, symbol, uuid)
);
