CREATE TABLE Financials (
  Date_YYYYMMDD DATE,
  symbol VARCHAR(255) PRIMARY KEY, -- symbol: 株式のシンボル（銘柄コード）
  revenue DECIMAL(18, 2), -- revenue: 売上高
  netIncomeToCommon DECIMAL(18, 2), -- netIncomeToCommon: 一般株主に対する純利益
  profitMargins DECIMAL(18, 2), -- profitMargins: 利益率
  trailingEps DECIMAL(18, 2), -- trailingEps: 直近の1株利益
  forwardEps DECIMAL(18, 2), -- forwardEps: 予想される将来の1株利益
  trailingPE DECIMAL(18, 2), -- trailingPE: 直近の株価収益率（P/E比）
  forwardPE DECIMAL(18, 2), -- forwardPE: 予想される将来の株価収益率（P/E比）
  dividendRate DECIMAL(18, 2), -- dividendRate: 配当金の額
  dividendYield DECIMAL(18, 2), -- dividendYield: 配当利回り
  payoutRatio DECIMAL(18, 2), -- payoutRatio: 配当利益率
  INS_DATE datetime, -- INS_DATE: データの挿入日時
  UPD_DATE datetime, -- UPD_DATE: データの更新日時
  PRIMARY KEY (`Date_YYYYMMDD`, `symbol`)
);
