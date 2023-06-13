CREATE TABLE RiskAssessment (
  symbol VARCHAR(255) PRIMARY KEY,
  auditRisk DECIMAL(18, 2),
  boardRisk DECIMAL(18, 2),
  compensationRisk DECIMAL(18, 2),
  shareHolderRightsRisk DECIMAL(18, 2),
  overallRisk DECIMAL(18, 2),
  INS_DATE datetime,
  UPD_DATE datetime
);