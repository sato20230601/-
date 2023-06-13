INSERT INTO Financials (
    symbol,
    revenue,
    netIncomeToCommon,
    profitMargins,
    trailingEps,
    forwardEps,
    trailingPE,
    forwardPE,
    dividendRate,
    dividendYield,
    payoutRatio,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);