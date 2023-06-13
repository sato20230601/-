INSERT INTO RiskAssessment (
    symbol,
    auditRisk,
    boardRisk,
    compensationRisk,
    shareHolderRightsRisk,
    overallRisk,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);