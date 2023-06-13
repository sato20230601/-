CREATE TABLE EcoCalendar (
  `No` INT, -- 月のイベントの順番
  `Date_ymd` DATE, -- 年月日
  `Event_name` VARCHAR(100), -- 経済イベント
  `Cur` VARCHAR(3), -- 通貨
  `Weekday` INT, -- 曜日
  `Time` VARCHAR(6), -- 時刻
  `Comp` VARCHAR(20), -- 前回比率
  `Mon` VARCHAR(20), -- 前回月
  `Last` VARCHAR(20), -- 前回値
  `Change` VARCHAR(20), -- 変動
  `Estimate` VARCHAR(20), -- 予想
  `Result` VARCHAR(20), -- 結果
  `Importance` INT, -- 重要度
  `Display` INT, -- 表示
  `INS_DATE` DATETIME, -- データ登録日時
  `UPD_DATE` DATETIME, -- データ更新日時
  PRIMARY KEY (`Date_ymd`, `Event_name`, `Cur`)
); -- 経済カレンダ情報 テーブル

