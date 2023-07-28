-- Cash Flow（キャッシュフロー）テーブル: ticker.get_cash_flow() 関数から取得した財務データのキャッシュフロー計算書
CREATE TABLE yf_cash_flow (
  Date_YYYYMMDD DATE, -- 日付 (YYYY-MM-DD)
  symbol VARCHAR(255), -- 銘柄コード
  FreeCashFlow FLOAT, -- 無料現金流
  RepurchaseOfCapitalStock FLOAT, -- 自己株式の取得
  RepaymentOfDebt FLOAT, -- 借金の返済
  IssuanceOfDebt FLOAT, -- 借金の発行
  CapitalExpenditure FLOAT, -- 資本支出
  IncomeTaxPaidSupplementalData FLOAT, -- 事業税支払い（補足データ）
  EndCashPosition FLOAT, -- 期末現金残高
  BeginningCashPosition FLOAT, -- 期首現金残高
  EffectOfExchangeRateChanges FLOAT, -- 為替レート変動の影響
  ChangesInCash FLOAT, -- 現金の変動
  FinancingCashFlow FLOAT, -- 財務キャッシュフロー
  CashFlowFromContinuingFinancingActivities FLOAT, -- 継続する財務活動からのキャッシュフロー
  NetOtherFinancingCharges FLOAT, -- その他の財務関連費用の純額
  ProceedsFromStockOptionExercised FLOAT, -- 株式オプションの行使による受取金
  NetCommonStockIssuance FLOAT, -- 普通株式の発行の純額
  CommonStockPayments FLOAT, -- 普通株式の支払い
  NetIssuancePaymentsOfDebt FLOAT, -- 借金の発行・返済の純額
  NetLongTermDebtIssuance FLOAT, -- 長期借金の発行の純額
  LongTermDebtPayments FLOAT, -- 長期借金の返済
  LongTermDebtIssuance FLOAT, -- 長期借金の発行
  InvestingCashFlow FLOAT, -- 投資キャッシュフロー
  CashFlowFromContinuingInvestingActivities FLOAT, -- 継続する投資活動からのキャッシュフロー
  NetOtherInvestingChanges FLOAT, -- その他の投資関連変動の純額
  NetInvestmentPurchaseAndSale FLOAT, -- 投資の購入・売却の純額
  SaleOfInvestment FLOAT, -- 投資の売却
  PurchaseOfInvestment FLOAT, -- 投資の購入
  NetBusinessPurchaseAndSale FLOAT, -- 事業の売却・購入の純額
  PurchaseOfBusiness FLOAT, -- 事業の購入
  NetIntangiblesPurchaseAndSale FLOAT, -- 無形資産の売却・購入の純額
  PurchaseOfIntangibles FLOAT, -- 無形資産の購入
  NetPPEPurchaseAndSale FLOAT, -- 固定資産の売却・購入の純額
  PurchaseOfPPE FLOAT, -- 固定資産の購入
  OperatingCashFlow FLOAT, -- オペレーティングキャッシュフロー
  CashFlowFromContinuingOperatingActivities FLOAT, -- 継続する営業活動からのキャッシュフロー
  ChangeInWorkingCapital FLOAT, -- 運転資本の変動
  ChangeInOtherWorkingCapital FLOAT, -- その他の運転資本の変動
  ChangeInOtherCurrentAssets FLOAT, -- その他の流動資産の変動
  ChangeInPayablesAndAccruedExpense FLOAT, -- 支払手形および未払費用の変動
  ChangeInAccruedExpense FLOAT, -- 未払費用の変動
  ChangeInPayable FLOAT, -- 支払手形の変動
  ChangeInAccountPayable FLOAT, -- 仕入債務の変動
  ChangeInReceivables FLOAT, -- 売掛金の変動
  ChangesInAccountReceivables FLOAT, -- 売掛金の変動
  OtherNonCashItems FLOAT, -- その他の非現金項目
  StockBasedCompensation FLOAT, -- 株式ベースの報酬
  DeferredTax FLOAT, -- 延期税金
  DeferredIncomeTax FLOAT, -- 延期所得税
  DepreciationAmortizationDepletion FLOAT, -- 減価償却・償却・減少
  DepreciationAndAmortization FLOAT, -- 減価償却・償却
  AmortizationCashFlow FLOAT, -- 償却のキャッシュフロー
  AmortizationOfIntangibles FLOAT, -- 無形資産の償却
  Depreciation FLOAT, -- 減価償却
  OperatingGainsLosses FLOAT, -- オペレーティング利益・損失
  GainLossOnInvestmentSecurities FLOAT, -- 投資証券の利益・損失
  NetIncomeFromContinuingOperations FLOAT, -- 継続する事業からの純利益
  INS_DATE DATETIME, -- 登録日時
  UPD_DATE DATETIME, -- 更新日時
  PRIMARY KEY (Date_YYYYMMDD, symbol)
);
