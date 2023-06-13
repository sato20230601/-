INSERT INTO EcoCalendar (
  `No`,
  `Date_ymd`,
  `Event_name`,
  `Cur`,
  `Weekday`,
  `Time`,
  `Comp`,
  `Mon`,
  `Last`,
  `Change`,
  `Estimate`,
  `Result`,
  `Importance`,
  `Display`,
  `INS_DATE`,
  `UPD_DATE`
)
VALUES (%s,'%s','%s','%s',%s,'%s','%s','%s','%s',%s,%s,%s,%s, %s,'%s','%s')
ON DUPLICATE KEY UPDATE
  `Weekday` = VALUES(`Weekday`),
  `Time` = VALUES(`Time`),
  `Comp` = VALUES(`Comp`),
  `Mon` = VALUES(`Mon`),
  `Last` = VALUES(`Last`),
  `Change` = VALUES(`Change`),
  `Estimate` = VALUES(`Estimate`),
  `Result` = VALUES(`Result`),
  `Importance` = VALUES(`Importance`),
  `Display` = VALUES(`Display`),
  `Upd_date` = VALUES(`UPD_DATE`);
