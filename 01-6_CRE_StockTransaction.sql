CREATE TABLE StockTransaction (
    symbol VARCHAR(10),                -- 銘柄
    No INT,                            -- No
    transaction_date DATE,             -- 取引日付
    transaction_type VARCHAR(10),      -- 取引種別
    market VARCHAR(10),                -- 市場
    settlement_currency VARCHAR(10),   -- 決済通貨
    exchange_rate DECIMAL(10, 3),      -- USドル/円（為替レート）
    unit_price DECIMAL(10, 5),         -- 単価［USドル］
    quantity INT,                      -- 数量［株］
    INS_DATE datetime,                 -- データ登録日時
    UPD_DATE datetime,                 -- データ更新日時
    PRIMARY KEY (symbol, No)            -- 複合主キーの設定
);

CREATE TRIGGER SetNoOnInsert
BEFORE INSERT ON StockTransaction
FOR EACH ROW
BEGIN
    SET NEW.No = (SELECT IFNULL(MAX(No) + 1, 1) FROM StockTransaction WHERE symbol = NEW.symbol);
END
