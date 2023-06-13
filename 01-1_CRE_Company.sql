CREATE TABLE Company (
  symbol VARCHAR(255) PRIMARY KEY,
  shortName VARCHAR(255),
  longName VARCHAR(255),
  industry VARCHAR(255),
  sector VARCHAR(255),
  fullTimeEmployees INT,
  website VARCHAR(255),
  INS_DATE datetime,
  UPD_DATE datetime
);
