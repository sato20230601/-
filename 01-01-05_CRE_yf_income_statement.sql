-- 収益計算書テーブル (income_statement)
CREATE TABLE yf_income_statement (
  Date_YYYYMMDD DATE, -- 日付 (YYYY-MM-DD)
  symbol VARCHAR(255), -- 株式のシンボル（銘柄コード）
  TaxEffectOfUnusualItems FLOAT, -- 異常なアイテムの税効果
  TaxRateForCalcs FLOAT, -- 計算用税率
  NormalizedEBITDA FLOAT, -- 正常化したEBITDA
  TotalUnusualItems FLOAT, -- 総異常アイテム
  TotalUnusualItemsExcludingGoodwill FLOAT, -- 善意を除く総異常アイテム
  NetIncomeFromContinuingOperationNetMinorityInterests FLOAT, -- 継続事業の純利益（少数利益を除く）
  ReconciledDepreciation FLOAT, -- 調整済み償却費
  ReconciledCostOfRevenue FLOAT, -- 調整済み売上原価
  EBIT FLOAT, -- EBIT（利息および税金を除く営業利益）
  NetInterestIncome FLOAT, -- 純利息収入
  InterestExpense FLOAT, -- 支払利息
  InterestIncome FLOAT, -- 受取利息
  NormalizedIncome FLOAT, -- 正常化した純利益
  NetIncomeFromContinuingAndDiscontinuedOperation FLOAT, -- 継続および中断事業からの純利益
  TotalExpenses FLOAT, -- 総費用
  TotalOperatingIncomeAsReported FLOAT, -- 営業利益（報告された値）
  DilutedAverageShares FLOAT, -- 希薄化調整後平均株式数
  BasicAverageShares FLOAT, -- 簡約調整後平均株式数
  DilutedEPS FLOAT, -- 希薄化調整後EPS（1株当たり利益）
  BasicEPS FLOAT, -- 簡約調整後EPS（1株当たり利益）
  DilutedNIAvailtoComStockholders FLOAT, -- 株主に帰属する希薄化調整後純利益
  NetIncomeCommonStockholders FLOAT, -- 株主に帰属する純利益
  NetIncome FLOAT, -- 純利益
  NetIncomeIncludingNoncontrollingInterests FLOAT, -- 非支配株主に帰属する純利益を含む純利益
  NetIncomeContinuousOperations FLOAT, -- 継続事業の純利益
  TaxProvision FLOAT, -- 税金引当額
  PretaxIncome FLOAT, -- 税引前利益
  OtherIncomeExpense FLOAT, -- その他の収益費用
  OtherNonOperatingIncomeExpenses FLOAT, -- その他の非営業収益費用
  SpecialIncomeCharges FLOAT, -- 特別収益費用
  OtherSpecialCharges FLOAT, -- その他の特別費用
  EarningsFromEquityInterest FLOAT, -- 持分法適用会社からの収益
  GainOnSaleOfSecurity FLOAT, -- 有価証券の売却益
  NetNonOperatingInterestIncomeExpense FLOAT, -- 純非営業利息収益費用
  TotalOtherFinanceCost FLOAT, -- その他の金融費用総額
  InterestExpenseNonOperating FLOAT, -- 非営業利息費用
  InterestIncomeNonOperating FLOAT, -- 非営業利息収入
  OperatingIncome FLOAT, -- 営業利益
  OperatingExpense FLOAT, -- 営業費用
  ResearchAndDevelopment FLOAT, -- 研究開発費
  SellingGeneralAndAdministration FLOAT, -- 販売一般および管理費用
  SellingAndMarketingExpense FLOAT, -- 販売およびマーケティング費用
  GeneralAndAdministrativeExpense FLOAT, -- 一般管理費用
  OtherGandA FLOAT, -- その他の一般管理費用
  GrossProfit FLOAT, -- 粗利益
  CostOfRevenue FLOAT, -- 売上原価
  TotalRevenue FLOAT, -- 総収益
  OperatingRevenue FLOAT, -- 営業収益
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  PRIMARY KEY (Date_YYYYMMDD, symbol)
);