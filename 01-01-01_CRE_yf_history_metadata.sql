CREATE TABLE yf_history_metadata (
    Date_YYYYMMDD DATE, -- 日付 (YYYY-MM-DD)
    symbol VARCHAR(255), -- 銘柄コード
    currency VARCHAR(10), -- 通貨
    exchangeName VARCHAR(255), -- 取引所名
    instrumentType VARCHAR(255), -- 取引対象の種類（株式、債券など）
    firstTradeDate TIMESTAMP, -- 最初の取引日時
    regularMarketTime TIMESTAMP, -- 通常取引の最終更新日時
    gmtoffset INT, -- GMTオフセット（秒単位）
    timezone VARCHAR(255), -- タイムゾーン
    exchangeTimezoneName VARCHAR(255), -- 取引所のタイムゾーン名
    regularMarketPrice FLOAT, -- 通常取引の現在価格
    chartPreviousClose FLOAT, -- チャートの前日終値
    priceHint INT, -- 価格のヒント
    preStart TIMESTAMP, -- 前取引期間の開始日時
    preEnd TIMESTAMP, -- 前取引期間の終了日時
    regularStart TIMESTAMP, -- 通常取引期間の開始日時
    regularEnd TIMESTAMP, -- 通常取引期間の終了日時
    postStart TIMESTAMP, -- 後取引期間の開始日時
    postEnd TIMESTAMP, -- 後取引期間の終了日時
    dataGranularity VARCHAR(10), -- データの粒度（1分ごと、1日ごとなど）
    validRanges TEXT, -- 有効なデータ範囲
    INS_DATE DATETIME, -- 登録日時
    UPD_DATE DATETIME, -- 更新日時
    PRIMARY KEY (Date_YYYYMMDD, symbol)
);
