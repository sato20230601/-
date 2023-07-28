CREATE TABLE yf_financials (
  Date_YYYYMMDD DATE, -- 日付 (YYYY-MM-DD)
  symbol VARCHAR(255), -- 銘柄コード
  TaxEffectOfUnusualItems FLOAT, -- 税金に対する特別項目の効果
  TaxRateForCalcs FLOAT, -- 計算のための税率
  NormalizedEBITDA FLOAT, -- 正規化されたEBITDA（税引き前の利益、減価償却費、利子費用、税金前利益）
  TotalUnusualItems FLOAT, -- 特別項目の合計
  TotalUnusualItemsExcludingGoodwill FLOAT, -- 親会社株主に帰属する特別項目の合計（のうちの営業外の特別項目を除く）
  NetIncomeFromContinuingOperationNetMinorityInterest FLOAT, -- 非中核事業を除く持分法適用関連企業を含む連結純利益
  ReconciledDepreciation FLOAT, -- 調整された減価償却費用
  ReconciledCostOfRevenue FLOAT, -- 調整された売上原価
  EBIT FLOAT, -- 税引き前利益（利子費用や税金を除いた利益）
  NetInterestIncome FLOAT, -- 正味利子収入
  InterestExpense FLOAT, -- 利子費用
  InterestIncome FLOAT, -- 利子収入
  NormalizedIncome FLOAT, -- 正規化された純利益
  NetIncomeFromContinuingAndDiscontinuedOperation FLOAT, -- 非中核事業を含む持分法適用関連企業を含む連結純利益
  TotalExpenses FLOAT, -- 経費の合計
  TotalOperatingIncomeAsReported FLOAT, -- 事業利益（報告値）
  DilutedAverageShares FLOAT, -- 稀 diluted EPS に用いられる平均稀株数（希薄化調整後）
  BasicAverageShares FLOAT, -- 希薄 diluted EPS に用いられる平均稀株数（希薄化調整前）
  DilutedEPS FLOAT, -- 稀 diluted EPS（希薄化調整後）
  BasicEPS FLOAT, -- 希薄 diluted EPS（希薄化調整前）
  DilutedNIAvailtoComStockholders FLOAT, -- 株主に帰属する稀 diluted 純利益（希薄化調整後）
  NetIncomeCommonStockholders FLOAT, -- 株主に帰属する純利益
  NetIncome FLOAT, -- 純利益
  NetIncomeIncludingNoncontrollingInterests FLOAT, -- 非支配株主持分を含む純利益
  NetIncomeContinuousOperations FLOAT, -- 連続営業純利益
  TaxProvision FLOAT, -- 税金引当金
  PretaxIncome FLOAT, -- 税引き前利益
  OtherIncomeExpense FLOAT, -- その他の収入・費用
  OtherNonOperatingIncomeExpenses FLOAT, -- その他の営業外収益・費用
  SpecialIncomeCharges FLOAT, -- 特別収益・費用
  OtherSpecialCharges FLOAT, -- その他の特別費用
  EarningsFromEquityInterest FLOAT, -- 持分法適用関連企業からの収益
  GainOnSaleOfSecurity FLOAT, -- 有価証券の売却益
  NetNonOperatingInterestIncomeExpense FLOAT, -- 正味営業外利子収益・費用
  TotalOtherFinanceCost FLOAT, -- その他の財務費用の合計
  InterestExpenseNonOperating FLOAT, -- 正味営業外利子費用
  InterestIncomeNonOperating FLOAT, -- 正味営業外利子収入
  OperatingIncome FLOAT, -- 事業利益
  OperatingExpense FLOAT, -- 営業費用
  ResearchAndDevelopment FLOAT, -- 研究開発費
  SellingGeneralAndAdministration FLOAT, -- 販売一般管理費用
  SellingAndMarketingExpense FLOAT, -- 販売・マーケティング費用
  GeneralAndAdministrativeExpense FLOAT, -- 一般管理費用
  OtherGandA FLOAT, -- その他の一般管理費用
  GrossProfit FLOAT, -- 粗利益
  CostOfRevenue FLOAT, -- 売上原価
  TotalRevenue FLOAT, -- 売上高の合計
  OperatingRevenue FLOAT, -- 営業収入
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  PRIMARY KEY (Date_YYYYMMDD, symbol)
);
