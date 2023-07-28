INSERT INTO yf_news (
    Date_YYYYMMDD,
    symbol,
    uuid,
    title,
    publisher,
    link,
    providerPublishTime,
    type,
    relatedTickers,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    title = VALUES(title),
    publisher = VALUES(publisher),
    link = VALUES(link),
    providerPublishTime = VALUES(providerPublishTime),
    type = VALUES(type),
    relatedTickers = VALUES(relatedTickers),
    UPD_DATE = VALUES(UPD_DATE);
