INSERT INTO yf_cash_flow (
    Date_YYYYMMDD,
    symbol,
    FreeCashFlow,
    RepurchaseOfCapitalStock,
    RepaymentOfDebt,
    IssuanceOfDebt,
    CapitalExpenditure,
    IncomeTaxPaidSupplementalData,
    EndCashPosition,
    BeginningCashPosition,
    EffectOfExchangeRateChanges,
    ChangesInCash,
    FinancingCashFlow,
    CashFlowFromContinuingFinancingActivities,
    NetOtherFinancingCharges,
    ProceedsFromStockOptionExercised,
    NetCommonStockIssuance,
    CommonStockPayments,
    NetIssuancePaymentsOfDebt,
    NetLongTermDebtIssuance,
    LongTermDebtPayments,
    LongTermDebtIssuance,
    InvestingCashFlow,
    CashFlowFromContinuingInvestingActivities,
    NetOtherInvestingChanges,
    NetInvestmentPurchaseAndSale,
    SaleOfInvestment,
    PurchaseOfInvestment,
    NetBusinessPurchaseAndSale,
    PurchaseOfBusiness,
    NetIntangiblesPurchaseAndSale,
    PurchaseOfIntangibles,
    NetPPEPurchaseAndSale,
    PurchaseOfPPE,
    OperatingCashFlow,
    CashFlowFromContinuingOperatingActivities,
    ChangeInWorkingCapital,
    ChangeInOtherWorkingCapital,
    ChangeInOtherCurrentAssets,
    ChangeInPayablesAndAccruedExpense,
    ChangeInAccruedExpense,
    ChangeInPayable,
    ChangeInAccountPayable,
    ChangeInReceivables,
    ChangesInAccountReceivables,
    OtherNonCashItems,
    StockBasedCompensation,
    DeferredTax,
    DeferredIncomeTax,
    DepreciationAmortizationDepletion,
    DepreciationAndAmortization,
    AmortizationCashFlow,
    AmortizationOfIntangibles,
    Depreciation,
    OperatingGainsLosses,
    GainLossOnInvestmentSecurities,
    NetIncomeFromContinuingOperations,
    INS_DATE,
    UPD_DATE
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    FreeCashFlow = VALUES(FreeCashFlow),
    RepurchaseOfCapitalStock = VALUES(RepurchaseOfCapitalStock),
    RepaymentOfDebt = VALUES(RepaymentOfDebt),
    IssuanceOfDebt = VALUES(IssuanceOfDebt),
    CapitalExpenditure = VALUES(CapitalExpenditure),
    IncomeTaxPaidSupplementalData = VALUES(IncomeTaxPaidSupplementalData),
    EndCashPosition = VALUES(EndCashPosition),
    BeginningCashPosition = VALUES(BeginningCashPosition),
    EffectOfExchangeRateChanges = VALUES(EffectOfExchangeRateChanges),
    ChangesInCash = VALUES(ChangesInCash),
    FinancingCashFlow = VALUES(FinancingCashFlow),
    CashFlowFromContinuingFinancingActivities = VALUES(CashFlowFromContinuingFinancingActivities),
    NetOtherFinancingCharges = VALUES(NetOtherFinancingCharges),
    ProceedsFromStockOptionExercised = VALUES(ProceedsFromStockOptionExercised),
    NetCommonStockIssuance = VALUES(NetCommonStockIssuance),
    CommonStockPayments = VALUES(CommonStockPayments),
    NetIssuancePaymentsOfDebt = VALUES(NetIssuancePaymentsOfDebt),
    NetLongTermDebtIssuance = VALUES(NetLongTermDebtIssuance),
    LongTermDebtPayments = VALUES(LongTermDebtPayments),
    LongTermDebtIssuance = VALUES(LongTermDebtIssuance),
    InvestingCashFlow = VALUES(InvestingCashFlow),
    CashFlowFromContinuingInvestingActivities = VALUES(CashFlowFromContinuingInvestingActivities),
    NetOtherInvestingChanges = VALUES(NetOtherInvestingChanges),
    NetInvestmentPurchaseAndSale = VALUES(NetInvestmentPurchaseAndSale),
    SaleOfInvestment = VALUES(SaleOfInvestment),
    PurchaseOfInvestment = VALUES(PurchaseOfInvestment),
    NetBusinessPurchaseAndSale = VALUES(NetBusinessPurchaseAndSale),
    PurchaseOfBusiness = VALUES(PurchaseOfBusiness),
    NetIntangiblesPurchaseAndSale = VALUES(NetIntangiblesPurchaseAndSale),
    PurchaseOfIntangibles = VALUES(PurchaseOfIntangibles),
    NetPPEPurchaseAndSale = VALUES(NetPPEPurchaseAndSale),
    PurchaseOfPPE = VALUES(PurchaseOfPPE),
    OperatingCashFlow = VALUES(OperatingCashFlow),
    CashFlowFromContinuingOperatingActivities = VALUES(CashFlowFromContinuingOperatingActivities),
    ChangeInWorkingCapital = VALUES(ChangeInWorkingCapital),
    ChangeInOtherWorkingCapital = VALUES(ChangeInOtherWorkingCapital),
    ChangeInOtherCurrentAssets = VALUES(ChangeInOtherCurrentAssets),
    ChangeInPayablesAndAccruedExpense = VALUES(ChangeInPayablesAndAccruedExpense),
    ChangeInAccruedExpense = VALUES(ChangeInAccruedExpense),
    ChangeInPayable = VALUES(ChangeInPayable),
    ChangeInAccountPayable = VALUES(ChangeInAccountPayable),
    ChangeInReceivables = VALUES(ChangeInReceivables),
    ChangesInAccountReceivables = VALUES(ChangesInAccountReceivables),
    OtherNonCashItems = VALUES(OtherNonCashItems),
    StockBasedCompensation = VALUES(StockBasedCompensation),
    DeferredTax = VALUES(DeferredTax),
    DeferredIncomeTax = VALUES(DeferredIncomeTax),
    DepreciationAmortizationDepletion = VALUES(DepreciationAmortizationDepletion),
    DepreciationAndAmortization = VALUES(DepreciationAndAmortization),
    AmortizationCashFlow = VALUES(AmortizationCashFlow),
    AmortizationOfIntangibles = VALUES(AmortizationOfIntangibles),
    Depreciation = VALUES(Depreciation),
    OperatingGainsLosses = VALUES(OperatingGainsLosses),
    GainLossOnInvestmentSecurities = VALUES(GainLossOnInvestmentSecurities),
    NetIncomeFromContinuingOperations = VALUES(NetIncomeFromContinuingOperations),
    UPD_DATE = VALUES(UPD_DATE);
