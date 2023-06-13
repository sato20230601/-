CREATE TABLE Financials (
  symbol VARCHAR(255) PRIMARY KEY,
  revenue DECIMAL(18, 2),
  netIncomeToCommon DECIMAL(18, 2),
  profitMargins DECIMAL(18, 2),
  trailingEps DECIMAL(18, 2),
  forwardEps DECIMAL(18, 2),
  trailingPE DECIMAL(18, 2),
  forwardPE DECIMAL(18, 2),
  dividendRate DECIMAL(18, 2),
  dividendYield DECIMAL(18, 2),
  payoutRatio DECIMAL(18, 2),
  INS_DATE datetime,
  UPD_DATE datetime
);