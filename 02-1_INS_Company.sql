INSERT INTO Company (
    symbol,
    shortName,
    longName,
    industry,
    sector,
    fullTimeEmployees,
    website,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
