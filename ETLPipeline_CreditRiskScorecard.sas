/**************************************************************************************************/
/*                                                                                                */
/*   Project : ETL Pipeline for Credit Risk Scorecard - Fannie Mae Single-Family Loan Data        */
/*   Program : ETLPipeline_CreditRiskScorecard.sas                                                */
/*   Author  : Anne M. Prihoda                                                                    */
/*                                                                                                */
/*   Purpose : ETL Pipeline to process each Fannie Mae Single-Family Loan Performance file        */
/*                                                                                                */
/*             - Read each gzipped source file                                                    */
/*             - Retain only the data fields required to build the loan-level base for            */
/*               the credit risk scorecard                                                        */
/*             - Collapse the monthly performance records to one row per loan                     */
/*             - Replace any identical file previously imported (no duplication)                  */
/*             - Append to the master summary table                                               */
/*             - Record the run in acquisition-quarter order                                      */
/*             - Release working storage                                                          */
/*                                                                                                */
/*   Data    : Fannie Mae Single-Family Loan Performance files                                    */
/*                                                                                                */
/*             - Pipe-delimited, 113 fields, no header row                                        */
/*             - Field 1 (Reference Pool ID) is not populated, so every record begins             */
/*               with a delimiter                                                                 */
/*             - Source files are gzipped locally and read directly through the FILENAME          */
/*               zip engine - the uncompressed data never occupies server storage                 */
/*                                                                                                */
/*   Output  : FNMAE.LoanMaster - one row per loan, accumulates across all files                  */
/*             FNMAE.RunLog     - the processing record: one row per source data set,             */
/*                                with running record count                                       */
/*                                                                                                */
/*   Default : Fannie Mae definition of a defaulted loan                                          */
/*                                                                                                */
/*             - Zero Balance Code in (02, 03, 09, 15)                                            */
/*               02 = Third Party Sale                                                            */
/*               03 = Short Sale                                                                  */
/*               09 = Deed-in-Lieu; REO Disposition                                               */
/*               15 = Non Performing Note Sale                                                    */
/*             - AND a non-null Disposition Date                                                  */
/*             - Code 06 is a repurchase, not a default                                           */
/*                                                                                                */
/**************************************************************************************************/


/**************************************************************************************************/
/*                                                                                                */
/*   Environment setup - base directory and library assignments                                   */
/*                                                                                                */
/**************************************************************************************************/

%LET root = /home/bvsierrap0;

LIBNAME RAW   "&root/RAW"   COMPRESS=YES;
LIBNAME FNMAE "&root/FNMAE" COMPRESS=YES;



/**************************************************************************************************/
/*                                                                                                */
/*   Session_Log_Capture macro compartmentalizes the code that writes the SAS session log         */
/*   to ImportSession.log. This is the session log, not FNMAE.RunLog                              */
/*   Option #1 - Create: starts a new log file, erasing any existing one. Starred -               */
/*               un-star once at the start of a new project log                                   */
/*   Option #2 - Continue: appends this sessions log to the existing file, creating it            */
/*               if it does not exist                                                             */
/*   First step, run %Session_Log_Capture; at the start of every ETL processing session            */
/*   The capture ends by itself when the SAS session closes                                       */
/*                                                                                                */
/**************************************************************************************************/

%MACRO Session_Log_Capture;

    /*   Option #1 - Create: new log file, erasing any existing one   */
    *PROC PRINTTO LOG="&root/ImportSession.log" NEW;
    *RUN;

    /*   Option #2 - Continue: append this sessions log to the existing file   */
    FILENAME ImpLog "&root/ImportSession.log" MOD;
    PROC PRINTTO LOG=ImpLog;
    RUN;

%MEND Session_Log_Capture;

%Session_Log_Capture;



/**************************************************************************************************/
/*                                                                                                */
/*   RunLog - created once, appended to thereafter                                                */
/*                                                                                                */
/**************************************************************************************************/

%MACRO CreateRunLog;
    %IF NOT %SYSFUNC(EXIST(FNMAE.RunLog)) %THEN %DO;
        DATA FNMAE.RunLog;
            ATTRIB
                RunSeq       LENGTH=8    LABEL="Sequence"              FORMAT=COMMA8.
                Quarter      LENGTH=$6   LABEL="Acquisition quarter"
                SourceFile   LENGTH=$40  LABEL="Source file"
                RawRows      LENGTH=8    LABEL="Monthly records read"  FORMAT=COMMA16.
                LoanRows     LENGTH=8    LABEL="Loans after collapse"  FORMAT=COMMA16.
                Defaults     LENGTH=8    LABEL="Defaults identified"   FORMAT=COMMA16.
                ProcessTime  LENGTH=8    LABEL="Processing time"       FORMAT=TIME12.2
                CumRawRows   LENGTH=8    LABEL="Cumulative records"    FORMAT=COMMA16.
            ;
            STOP;
        RUN;
        %PUT NOTE: FNMAE.RunLog created.;
    %END;
%MEND CreateRunLog;


/**************************************************************************************************/
/*                                                                                                */
/*   ImportQuarterlyFile - the entire per-file routine                                            */
/*                                                                                                */
/**************************************************************************************************/

%MACRO ImportQuarterlyFile(File=, SrcDir=&root/RAW);

    %LOCAL YearQtr AcqQtr Dsid Rc T0 T1 Elapsed RawRows LoanRows Defaults Cum PriorRuns HasSrc HasPt HasFico;

    %LET YearQtr = %SCAN(&File, 1, .);
    %LET AcqQtr  = %SUBSTR(&YearQtr, 1, 6);
    %LET T0  = %SYSFUNC(DATETIME());

    /**********************************************************/
    %PUT NOTE: Processing data for the yearly quarter: &YearQtr;
    /**********************************************************/

    /**********************************************************************************************/
    /**** Step 0: Rebuild the master tables once if their layout predates this program ************/
    /**********************************************************************************************/
    %LET HasSrc  = 1;
    %LET HasPt   = 1;
    %LET HasFico = 1;

    %IF %SYSFUNC(EXIST(FNMAE.LoanMaster)) %THEN %DO;
        %LET Dsid    = %SYSFUNC(OPEN(FNMAE.LoanMaster));
        %LET HasSrc  = %SYSFUNC(VARNUM(&Dsid, SourceFile));
        %LET HasFico = %SYSFUNC(VARNUM(&Dsid, FICO));
        %LET Rc      = %SYSFUNC(CLOSE(&Dsid));
    %END;

    %IF %SYSFUNC(EXIST(FNMAE.RunLog)) %THEN %DO;
        %LET Dsid  = %SYSFUNC(OPEN(FNMAE.RunLog));
        %LET HasPt = %SYSFUNC(VARNUM(&Dsid, ProcessTime));
        %LET Rc    = %SYSFUNC(CLOSE(&Dsid));
    %END;

    %IF &HasSrc = 0 OR &HasPt = 0 OR &HasFico = 0 %THEN %DO;

        /***************************************************************/
        %PUT NOTE: Master tables predate the current layout - rebuilding;
        /***************************************************************/

        PROC DATASETS LIBRARY=FNMAE NOLIST;
            DELETE LoanMaster RunLog;
        QUIT;

        %CreateRunLog;
    %END;

    /**********************************************************************************************/
    /**** Step 1: Read the gzipped source, keeping only the modelling fields **********************/
    /**********************************************************************************************/
    FILENAME src ZIP "&SrcDir./&File" GZIP;

    DATA WORK._Monthly
         (COMPRESS=YES
          KEEP = LoanIdentifier MonthlyReportingPeriod Channel SellerName
                OriginalInterestRate OriginalUpb OriginalLoanTerm
                OriginationDate FirstPaymentDate
                OriginalLoanToValueRatio OriginalCombinedLoanToValueRatio
                NumberOfBorrowers DebtToIncome
                BorrowerCreditScoreAtOrigination CoBorrowerCreditScoreAtOriginati
                FirstTimeHomeBuyerIndicator LoanPurpose PropertyType
                NumberOfUnits OccupancyStatus PropertyState
                MetropolitanStatisticalAreaOrMet ZipCodeShort
                MortgageInsurancePercentage MortgageInsuranceType
                HighBalanceLoanIndicator PropertyValuationMethod
                RelocationMortgageIndicator SpecialEligibilityProgram
                CurrentLoanDelinquencyStatus ModificationFlag
                ZeroBalanceCode ZeroBalanceEffectiveDate
                DispositionDate UpbAtTheTimeOfRemoval CurrentActualUpb);
        INFILE src DLM='|' DSD MISSOVER LRECL=32767;

        ATTRIB
    ReferencePoolId                   LENGTH=$4     INFORMAT=$4.        FORMAT=$4.    /* not populated */
    LoanIdentifier                    LENGTH=$12    INFORMAT=$12.       FORMAT=$12.
    MonthlyReportingPeriod            LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7.
    Channel                           LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    SellerName                        LENGTH=$50    INFORMAT=$50.       FORMAT=$50.
    ServicerName                      LENGTH=$50    INFORMAT=$50.       FORMAT=$50.
    MasterServicer                    LENGTH=$10    INFORMAT=$10.       FORMAT=$10.   /* not populated */
    OriginalInterestRate              LENGTH=8      INFORMAT=BEST12.    FORMAT=12.3
    CurrentInterestRate               LENGTH=8      INFORMAT=BEST12.    FORMAT=12.3
    OriginalUpb                       LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    UpbAtIssuance                     LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    CurrentActualUpb                  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    OriginalLoanTerm                  LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    OriginationDate                   LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7.
    FirstPaymentDate                  LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7.
    LoanAge                           LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    RemainingMonthsToLegalMaturity    LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    RemainingMonthsToMaturity         LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    MaturityDate                      LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7.
    OriginalLoanToValueRatio          LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    OriginalCombinedLoanToValueRatio  LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    NumberOfBorrowers                 LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    DebtToIncome                      LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    BorrowerCreditScoreAtOrigination  LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    CoBorrowerCreditScoreAtOriginati  LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    FirstTimeHomeBuyerIndicator       LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    LoanPurpose                       LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    PropertyType                      LENGTH=$2     INFORMAT=$2.        FORMAT=$2.
    NumberOfUnits                     LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    OccupancyStatus                   LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    PropertyState                     LENGTH=$2     INFORMAT=$2.        FORMAT=$2.
    MetropolitanStatisticalAreaOrMet  LENGTH=$5     INFORMAT=$5.        FORMAT=$5.
    ZipCodeShort                      LENGTH=$3     INFORMAT=$3.        FORMAT=$3.
    MortgageInsurancePercentage       LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    AmortizationType                  LENGTH=$3     INFORMAT=$3.        FORMAT=$3.
    PrepaymentPenaltyIndicator        LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    InterestOnlyLoanIndicator         LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    InterestOnlyFirstPrincipalAndInt  LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7.
    MonthsToAmortization              LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    CurrentLoanDelinquencyStatus      LENGTH=$2     INFORMAT=$2.        FORMAT=$2.
    LoanPaymentHistory                LENGTH=$48    INFORMAT=$48.       FORMAT=$48.
    ModificationFlag                  LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    MortgageInsuranceCancellationInd  LENGTH=$2     INFORMAT=$2.        FORMAT=$2.    /* not populated */
    ZeroBalanceCode                   LENGTH=$3     INFORMAT=$3.        FORMAT=$3.
    ZeroBalanceEffectiveDate          LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7.
    UpbAtTheTimeOfRemoval             LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    RepurchaseDate                    LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7./* not populated */
    ScheduledPrincipalCurrent         LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    TotalPrincipalCurrent             LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    UnscheduledPrincipalCurrent       LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    LastPaidInstallmentDate           LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7.
    ForeclosureDate                   LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7.
    DispositionDate                   LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7.
    ForeclosureCosts                  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    PropertyPreservationAndRepairCos  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    AssetRecoveryCosts                LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    MiscellaneousHoldingExpensesAndC  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    AssociatedTaxesForHoldingPropert  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    NetSalesProceeds                  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    CreditEnhancementProceeds         LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    RepurchaseMakeWholeProceeds       LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    OtherForeclosureProceeds          LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    ModificationRelatedNonInterestBe  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    PrincipalForgivenessAmount        LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    OriginalListStartDate             LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7./* not populated */
    OriginalListPrice                 LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    CurrentListStartDate              LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7./* not populated */
    CurrentListPrice                  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    BorrowerCreditScoreAtIssuance     LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12./* not populated */
    CoBorrowerCreditScoreAtIssuance   LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12./* not populated */
    BorrowerCreditScoreCurrent        LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12./* not populated */
    CoBorrowerCreditScoreCurrent      LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12./* not populated */
    MortgageInsuranceType             LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    ServicingActivityIndicator        LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    CurrentPeriodModificationLossAmo  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    CumulativeModificationLossAmount  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    CurrentPeriodCreditEventNetGainO  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    CumulativeCreditEventNetGainOrLo  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    SpecialEligibilityProgram         LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    ForeclosurePrincipalWriteOffAmou  LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    RelocationMortgageIndicator       LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    ZeroBalanceCodeChangeDate         LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7./* not populated */
    LoanHoldbackIndicator             LENGTH=$1     INFORMAT=$1.        FORMAT=$1.    /* not populated */
    LoanHoldbackEffectiveDate         LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7./* not populated */
    DelinquentAccruedInterest         LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    PropertyValuationMethod           LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    HighBalanceLoanIndicator          LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    ArmInitialFixedRatePeriod5YrIndi  LENGTH=$1     INFORMAT=$1.        FORMAT=$1.    /* not populated */
    ArmProductType                    LENGTH=$100   INFORMAT=$100.      FORMAT=$100.  /* not populated */
    InitialFixedRatePeriod            LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12./* not populated */
    InterestRateAdjustmentFrequency   LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12./* not populated */
    NextInterestRateAdjustmentDate    LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7./* not populated */
    NextPaymentChangeDate             LENGTH=8      INFORMAT=BEST12.    FORMAT=MMYYS7./* not populated */
    Index                             LENGTH=$100   INFORMAT=$100.      FORMAT=$100.  /* not populated */
    ArmCapStructure                   LENGTH=$10    INFORMAT=$10.       FORMAT=$10.   /* not populated */
    InitialInterestRateCapUpPercent   LENGTH=8      INFORMAT=BEST12.    FORMAT=12.4   /* not populated */
    PeriodicInterestRateCapUpPercent  LENGTH=8      INFORMAT=BEST12.    FORMAT=12.4   /* not populated */
    LifetimeInterestRateCapUpPercent  LENGTH=8      INFORMAT=BEST12.    FORMAT=12.4   /* not populated */
    MortgageMargin                    LENGTH=8      INFORMAT=BEST12.    FORMAT=12.4   /* not populated */
    ArmBalloonIndicator               LENGTH=$1     INFORMAT=$1.        FORMAT=$1.    /* not populated */
    ArmPlanNumber                     LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12./* not populated */
    BorrowerAssistancePlan            LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    HighLoanToValueRefinanceOptionIn  LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    DealName                          LENGTH=$200   INFORMAT=$200.      FORMAT=$200.  /* not populated */
    RepurchaseMakeWholeProceedsFlag   LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    AlternativeDelinquencyResolution  LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    AlternativeDelinquencyResoluti2   LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    TotalDeferralAmount               LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2
    PaymentDeferralModificationEvent  LENGTH=$1     INFORMAT=$1.        FORMAT=$1.
    InterestBearingUpb                LENGTH=8      INFORMAT=BEST12.    FORMAT=14.2   /* not populated */
    OriginationClassicFico            LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12.
    IssuanceClassicFico               LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12./* not populated */
    CurrentClassicFico                LENGTH=8      INFORMAT=BEST12.    FORMAT=BEST12./* not populated */
        ;

        INPUT
    ReferencePoolId LoanIdentifier MonthlyReportingPeriod Channel SellerName
    ServicerName MasterServicer OriginalInterestRate CurrentInterestRate OriginalUpb
    UpbAtIssuance CurrentActualUpb OriginalLoanTerm OriginationDate FirstPaymentDate
    LoanAge RemainingMonthsToLegalMaturity RemainingMonthsToMaturity MaturityDate
    OriginalLoanToValueRatio OriginalCombinedLoanToValueRatio NumberOfBorrowers
    DebtToIncome BorrowerCreditScoreAtOrigination CoBorrowerCreditScoreAtOriginati
    FirstTimeHomeBuyerIndicator LoanPurpose PropertyType NumberOfUnits OccupancyStatus
    PropertyState MetropolitanStatisticalAreaOrMet ZipCodeShort MortgageInsurancePercentage
    AmortizationType PrepaymentPenaltyIndicator InterestOnlyLoanIndicator
    InterestOnlyFirstPrincipalAndInt MonthsToAmortization CurrentLoanDelinquencyStatus
    LoanPaymentHistory ModificationFlag MortgageInsuranceCancellationInd ZeroBalanceCode
    ZeroBalanceEffectiveDate UpbAtTheTimeOfRemoval RepurchaseDate
    ScheduledPrincipalCurrent TotalPrincipalCurrent UnscheduledPrincipalCurrent
    LastPaidInstallmentDate ForeclosureDate DispositionDate ForeclosureCosts
    PropertyPreservationAndRepairCos AssetRecoveryCosts MiscellaneousHoldingExpensesAndC
    AssociatedTaxesForHoldingPropert NetSalesProceeds CreditEnhancementProceeds
    RepurchaseMakeWholeProceeds OtherForeclosureProceeds ModificationRelatedNonInterestBe
    PrincipalForgivenessAmount OriginalListStartDate OriginalListPrice
    CurrentListStartDate CurrentListPrice BorrowerCreditScoreAtIssuance
    CoBorrowerCreditScoreAtIssuance BorrowerCreditScoreCurrent CoBorrowerCreditScoreCurrent
    MortgageInsuranceType ServicingActivityIndicator CurrentPeriodModificationLossAmo
    CumulativeModificationLossAmount CurrentPeriodCreditEventNetGainO CumulativeCreditEventNetGainOrLo
    SpecialEligibilityProgram ForeclosurePrincipalWriteOffAmou RelocationMortgageIndicator
    ZeroBalanceCodeChangeDate LoanHoldbackIndicator LoanHoldbackEffectiveDate
    DelinquentAccruedInterest PropertyValuationMethod HighBalanceLoanIndicator
    ArmInitialFixedRatePeriod5YrIndi ArmProductType InitialFixedRatePeriod
    InterestRateAdjustmentFrequency NextInterestRateAdjustmentDate NextPaymentChangeDate
    Index ArmCapStructure InitialInterestRateCapUpPercent PeriodicInterestRateCapUpPercent
    LifetimeInterestRateCapUpPercent MortgageMargin ArmBalloonIndicator ArmPlanNumber
    BorrowerAssistancePlan HighLoanToValueRefinanceOptionIn DealName
    RepurchaseMakeWholeProceedsFlag AlternativeDelinquencyResolution AlternativeDelinquencyResoluti2
    TotalDeferralAmount PaymentDeferralModificationEvent InterestBearingUpb
    OriginationClassicFico IssuanceClassicFico CurrentClassicFico
        ;

        /*  Dates arrive as MMYYYY digits (122025 = December 2025). Convert to                    */
        /*  SAS date values so that MMYYS7. displays correctly and INTCK / INTNX                  */
        /*  and numeric sorting all operate on true dates.                                        */
        ARRAY _dt {*}  MonthlyReportingPeriod OriginationDate FirstPaymentDate
                       MaturityDate InterestOnlyFirstPrincipalAndInt
                       ZeroBalanceEffectiveDate RepurchaseDate
                       LastPaidInstallmentDate ForeclosureDate DispositionDate
                       OriginalListStartDate CurrentListStartDate
                       ZeroBalanceCodeChangeDate LoanHoldbackEffectiveDate
                       NextInterestRateAdjustmentDate NextPaymentChangeDate;

        DO _i = 1 to DIM(_dt);
            IF _dt{_i} > 9999 THEN
                _dt{_i} = mdy(int(_dt{_i}/10000), 1, mod(_dt{_i}, 10000));
            ELSE _dt{_i} = .;
        END;

        DROP _i;
        /**** Verify the source arrives sorted by loan - the collapse depends on it ***************/
        LENGTH PrevLoanId $12;
        RETAIN PrevLoanId;

        IF LoanIdentifier < PrevLoanId THEN DO;
            PUT "ERROR: Loan identifier out of sequence at record " _N_ COMMA16.;
            PUT "ERROR:   previous = " PrevLoanId "  current = " LoanIdentifier;
            ABORT CANCEL;
        END;

        PrevLoanId = LoanIdentifier;
        DROP PrevLoanId;
    RUN;

    FILENAME src CLEAR;

    %LET Dsid     = %SYSFUNC(OPEN(WORK._Monthly));
    %LET RawRows = %SYSFUNC(ATTRN(&Dsid, NLOBS));
    %LET Rc       = %SYSFUNC(CLOSE(&Dsid));

    /**********************************************************************************************/
    /**** Step 2: Collapse to one row per loan - no sort, groups are contiguous *******************/
    /**********************************************************************************************/
    DATA FNMAE.Loan&YearQtr;
        SET WORK._Monthly;
        BY LoanIdentifier NOTSORTED;

        RETAIN MaxDlq MonthsObserved EverD180 FirstD180Date
               EverModified LastZbCode LastZbDate LastDispDate
               UpbAtRemoval;

        ATTRIB
            FICO              LENGTH=8   LABEL="Borrower minimum credit score (FICO)" FORMAT=BEST12.
            OrigHomeValue    LENGTH=8   LABEL="Original home value"                FORMAT=14.2
            MaxDlq            LENGTH=8   LABEL="Maximum delinquency months"         FORMAT=BEST12.
            MonthsObserved    LENGTH=8   LABEL="Monthly records observed"           FORMAT=BEST12.
            EverD180          LENGTH=3   LABEL="Ever 180+ days delinquent"          FORMAT=BEST12.
            FirstD180Date    LENGTH=8   LABEL="First 180-day delinquency date"     FORMAT=MMYYS7.
            EverModified      LENGTH=3   LABEL="Ever modified"                      FORMAT=BEST12.
            LastZbCode       LENGTH=$3  LABEL="Final zero balance code"
            LastZbDate       LENGTH=8   LABEL="Final zero balance effective date"  FORMAT=MMYYS7.
            LastDispDate     LENGTH=8   LABEL="Final disposition date"             FORMAT=MMYYS7.
            UpbAtRemoval     LENGTH=8   LABEL="UPB at time of removal"             FORMAT=14.2
            DefaultFlag       LENGTH=3   LABEL="Default (ZB 02/03/09/15 + disp date)" FORMAT=BEST12.
            OrigYear          LENGTH=8   LABEL="Origination year"                   FORMAT=BEST12.
            OrigQuarter       LENGTH=$6  LABEL="Origination quarter"
            AcqQuarter        LENGTH=$6  LABEL="Acquisition quarter"
            SourceFile        LENGTH=$40 LABEL="Source file"
        ;

        IF first.LoanIdentifier THEN DO;
            MaxDlq = 0;  MonthsObserved = 0;  EverD180 = 0;  FirstD180Date = .;
            EverModified = 0;  LastZbCode = '';  LastZbDate = .;
            LastDispDate = .;  UpbAtRemoval = .;
        END;

        MonthsObserved + 1;

        /* 'XX' indicates delinquency status unavailable - treat as missing                       */
        IF CurrentLoanDelinquencyStatus not in ('XX','') THEN DO;
            IF INPUT(CurrentLoanDelinquencyStatus, 2.) > MaxDlq THEN
                MaxDlq = INPUT(CurrentLoanDelinquencyStatus, 2.);
            IF INPUT(CurrentLoanDelinquencyStatus, 2.) >= 6 and EverD180 = 0 THEN DO;
                EverD180       = 1;
                FirstD180Date = MonthlyReportingPeriod;
            END;
        END;

        IF ModificationFlag = 'Y' THEN EverModified = 1;

        /* terminal status values carry forward from whichever row holds them                     */
        IF ZeroBalanceCode           ne ''  THEN LastZbCode   = ZeroBalanceCode;
        IF ZeroBalanceEffectiveDate ne .   THEN LastZbDate   = ZeroBalanceEffectiveDate;
        IF DispositionDate            ne .   THEN LastDispDate = DispositionDate;
        IF UpbAtTheTimeOfRemoval  ne .   THEN UpbAtRemoval = UpbAtTheTimeOfRemoval;

        IF last.LoanIdentifier THEN DO;
            FICO = min(BorrowerCreditScoreAtOrigination,
                        CoBorrowerCreditScoreAtOriginati);
            IF OriginalLoanToValueRatio > 0 THEN
                OrigHomeValue = OriginalUpb / (OriginalLoanToValueRatio / 100);
            DefaultFlag = (LastZbCode in ('02','03','09','15') and LastDispDate ne .);
            OrigYear    = year(OriginationDate);
            OrigQuarter = cats(put(year(OriginationDate), 4.), 'Q',
                                put(qtr(OriginationDate), 1.));
            AcqQuarter  = "&AcqQtr";
            SourceFile  = "&File";
            OUTPUT;
        END;

        RENAME OriginalLoanToValueRatio         = LTV
               OriginalCombinedLoanToValueRatio = CLTV
               DebtToIncome                      = DTI
               OriginalUpb                       = OrigUPB
               OriginalInterestRate              = NoteRate
               NumberOfBorrowers                 = NumBorrower
               NumberOfUnits                     = NumUnits
               OriginalLoanTerm                  = OrigLoanTerm
               MortgageInsurancePercentage       = MortgInsPrct
               FirstTimeHomeBuyerIndicator       = FirstTimeBuyer
               MetropolitanStatisticalAreaOrMet  = MSA;

        DROP MonthlyReportingPeriod CurrentLoanDelinquencyStatus
             ModificationFlag ZeroBalanceCode ZeroBalanceEffectiveDate
             DispositionDate UpbAtTheTimeOfRemoval CurrentActualUpb;
    RUN;

    %LET Dsid      = %SYSFUNC(OPEN(FNMAE.Loan&YearQtr));
    %LET LoanRows = %SYSFUNC(ATTRN(&Dsid, NLOBS));
    %LET Rc        = %SYSFUNC(CLOSE(&Dsid));

    PROC SQL NOPRINT;
        SELECT sum(DefaultFlag) INTO :Defaults trimmed FROM FNMAE.Loan&YearQtr;
    QUIT;

    /**********************************************************************************************/
    /**** Step 3: Remove any prior load of this source file before appending the new one **********/
    /**********************************************************************************************/
    %LET PriorRuns = 0;

    %IF %SYSFUNC(EXIST(FNMAE.RunLog)) %THEN %DO;
        PROC SQL NOPRINT;
            SELECT count(*) INTO :PriorRuns trimmed
            FROM FNMAE.RunLog
            WHERE SourceFile = "&File";
        QUIT;
    %END;

    %IF &PriorRuns > 0 %THEN %DO;

        /******************************************************************/
        %PUT NOTE: &File was loaded previously - replacing the prior import;
        /******************************************************************/

        PROC SQL;
            DELETE FROM FNMAE.RunLog WHERE SourceFile = "&File";
        QUIT;

        %IF %SYSFUNC(EXIST(FNMAE.LoanMaster)) %THEN %DO;
            PROC SQL;
                DELETE FROM FNMAE.LoanMaster WHERE SourceFile = "&File";
            QUIT;
        %END;
    %END;

    /**********************************************************************************************/
    /**** Step 4: Append to the master analytical table *******************************************/
    /**********************************************************************************************/
    PROC APPEND BASE=FNMAE.LoanMaster DATA=FNMAE.Loan&YearQtr FORCE;
    RUN;

    /**********************************************************************************************/
    /**** Step 5: Record the run ******************************************************************/
    /**********************************************************************************************/
    %LET T1      = %SYSFUNC(DATETIME());
    %LET Elapsed = %SYSEVALF(&T1 - &T0);

    DATA WORK._logrow;
        ATTRIB
            RunSeq      LENGTH=8    FORMAT=COMMA8.
            Quarter      LENGTH=$6
            SourceFile  LENGTH=$40
            RawRows     LENGTH=8    FORMAT=COMMA16.
            LoanRows    LENGTH=8    FORMAT=COMMA16.
            Defaults     LENGTH=8    FORMAT=COMMA16.
            ProcessTime LENGTH=8    FORMAT=TIME12.2
            CumRawRows LENGTH=8    FORMAT=COMMA16.
        ;
        RunSeq = .;             Quarter      = "&AcqQtr";
        SourceFile = "&File";   RawRows     = &RawRows;
        LoanRows = &LoanRows;  Defaults     = &Defaults;
        ProcessTime = &Elapsed;
        CumRawRows = .;
        OUTPUT;
    RUN;

    PROC APPEND BASE=FNMAE.RunLog DATA=WORK._logrow FORCE;
    RUN;

    /**********************************************************************************************/
    /**** Step 6: Sort the run log oldest to newest, restate sequence and cumulative count ********/
    /**********************************************************************************************/
    PROC SORT DATA=FNMAE.RunLog;
        BY Quarter SourceFile;
    RUN;

    DATA FNMAE.RunLog;
        SET FNMAE.RunLog;
        RETAIN _cum 0;
        _cum + RawRows;
        CumRawRows = _cum;
        RunSeq      = _N_;
        DROP _cum;
    RUN;

    PROC SQL NOPRINT;
        SELECT sum(RawRows) INTO :Cum trimmed FROM FNMAE.RunLog;
    QUIT;

    /**********************************************************************************************/
    /**** Step 7: Release working storage *********************************************************/
    /**********************************************************************************************/
    PROC DATASETS LIBRARY=FNMAE NOLIST;  DELETE Loan&YearQtr;      QUIT;
    PROC DATASETS LIBRARY=WORK  NOLIST;  DELETE _Monthly _logrow;     QUIT;

    /**********************************************************************************************/
    /**** Step 8: Report **************************************************************************/
    /**********************************************************************************************/
    %PUT NOTE: &YearQtr complete - records read %SYSFUNC(PUTN(&RawRows, COMMA16.))
         - loans %SYSFUNC(PUTN(&LoanRows, COMMA16.))
         - Defaults %SYSFUNC(PUTN(&Defaults, COMMA16.))
         - Elapsed %SYSFUNC(PUTN(&Elapsed, TIME12.2))
         - CUMULATIVE RECORDS %SYSFUNC(PUTN(&Cum, COMMA16.));

%MEND ImportQuarterlyFile;

/**************************************************************************************************/
/*                                                                                                */
/*   DeleteSourceFile - remove a processed source file from RAW                                   */
/*                                                                                                */
/**************************************************************************************************/

%MACRO DeleteSourceFile(File=, SrcDir=&root/RAW);
    %LOCAL Rc Fref;
    %LET Fref = _dsrc;

    %IF %SYSFUNC(FILEEXIST(&SrcDir./&File)) %THEN %DO;
        %LET Rc = %SYSFUNC(FILENAME(Fref, &SrcDir./&File));
        %LET Rc = %SYSFUNC(FDELETE(&Fref));
        %LET Rc = %SYSFUNC(FILENAME(Fref));

        /********************************/
        %PUT NOTE: &File removed from RAW;
        /********************************/
    %END;
%MEND DeleteSourceFile;


/**************************************************************************************************/
/*                                                                                                */
/*   ProcessYearlyQuarter - one call per quarterly file: log, import, clean up                    */
/*                                                                                                */
/**************************************************************************************************/

%MACRO ProcessYearlyQuarter(YearQtr=);
    %LOCAL SrcFile;

    %LET SYSCC   = 0;
    %LET SrcFile = &YearQtr..csv.gz;

    %CreateRunLog;

    %ImportQuarterlyFile(File=&SrcFile);

    %IF &SYSCC <= 4 %THEN %DO;
        %DeleteSourceFile(File=&SrcFile);
    %END;
    %ELSE %DO;
        /********************************************************************/
        %PUT ERROR: &SrcFile was not processed cleanly - source file kept in RAW;
        /********************************************************************/
    %END;
%MEND ProcessYearlyQuarter;

/**************************************************************************************************/
/*                                                                                                */
/*   ExportRunLog - write the run log as a Markdown table for the project README                  */
/*                                                                                                */
/**************************************************************************************************/

%MACRO ExportRunLog(OutFile=&root/RunLog.md);
    DATA _NULL_;
        FILE "&OutFile";
        SET FNMAE.RunLog;
        LENGTH Line $200;

        IF _N_ = 1 THEN DO;
            PUT "| Quarter | Source file | Monthly records | Loans | Defaults | Process time | Cumulative records |";
            PUT "|:--|:--|--:|--:|--:|--:|--:|";
        END;

        Line = CATX(" | ", Quarter, SourceFile,
                    PUT(RawRows, COMMA16.), PUT(LoanRows, COMMA12.),
                    PUT(Defaults, COMMA10.), PUT(ProcessTime, TIME12.2),
                    PUT(CumRawRows, COMMA16.));
        PUT "| " Line +(-1) " |";
    RUN;

    /**************************************/
    %PUT NOTE: Run log exported to &OutFile;
    /**************************************/
%MEND ExportRunLog;



/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/

%ProcessYearlyQuarter(YearQtr=2000Q1);
%ProcessYearlyQuarter(YearQtr=2000Q2);
%ProcessYearlyQuarter(YearQtr=2000Q3);
%ProcessYearlyQuarter(YearQtr=2000Q4);
%ProcessYearlyQuarter(YearQtr=2001Q1);
%ProcessYearlyQuarter(YearQtr=2001Q2);
%ProcessYearlyQuarter(YearQtr=2001Q3);
%ProcessYearlyQuarter(YearQtr=2001Q4);
%ProcessYearlyQuarter(YearQtr=2002Q1);
%ProcessYearlyQuarter(YearQtr=2002Q2);
%ProcessYearlyQuarter(YearQtr=2002Q3);
%ProcessYearlyQuarter(YearQtr=2002Q4_01);
%ProcessYearlyQuarter(YearQtr=2002Q4_02);
%ProcessYearlyQuarter(YearQtr=2002Q4_03);
%ProcessYearlyQuarter(YearQtr=2003Q1_01);
%ProcessYearlyQuarter(YearQtr=2003Q1_02);
%ProcessYearlyQuarter(YearQtr=2003Q1_03);
%ProcessYearlyQuarter(YearQtr=2003Q1_04);
%ProcessYearlyQuarter(YearQtr=2003Q2_01);
%ProcessYearlyQuarter(YearQtr=2003Q2_02);
%ProcessYearlyQuarter(YearQtr=2003Q2_03);
%ProcessYearlyQuarter(YearQtr=2003Q2_04);
%ProcessYearlyQuarter(YearQtr=2003Q2_05);
%ProcessYearlyQuarter(YearQtr=2003Q3_01);
%ProcessYearlyQuarter(YearQtr=2003Q3_02);
%ProcessYearlyQuarter(YearQtr=2003Q3_03);
%ProcessYearlyQuarter(YearQtr=2003Q3_04);
%ProcessYearlyQuarter(YearQtr=2003Q3_05);
%ProcessYearlyQuarter(YearQtr=2003Q3_06);
%ProcessYearlyQuarter(YearQtr=2003Q4_01);
%ProcessYearlyQuarter(YearQtr=2003Q4_02);
%ProcessYearlyQuarter(YearQtr=2003Q4_03);
%ProcessYearlyQuarter(YearQtr=2004Q1);
%ProcessYearlyQuarter(YearQtr=2004Q2);
%ProcessYearlyQuarter(YearQtr=2004Q3);
%ProcessYearlyQuarter(YearQtr=2004Q4);
%ProcessYearlyQuarter(YearQtr=2005Q1);
%ProcessYearlyQuarter(YearQtr=2005Q2);
%ProcessYearlyQuarter(YearQtr=2005Q3);
%ProcessYearlyQuarter(YearQtr=2005Q4);
%ProcessYearlyQuarter(YearQtr=2006Q1);
%ProcessYearlyQuarter(YearQtr=2006Q2);
%ProcessYearlyQuarter(YearQtr=2006Q3);
%ProcessYearlyQuarter(YearQtr=2006Q4);
%ProcessYearlyQuarter(YearQtr=2007Q1);
%ProcessYearlyQuarter(YearQtr=2007Q2);
%ProcessYearlyQuarter(YearQtr=2007Q3);
%ProcessYearlyQuarter(YearQtr=2007Q4);

/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/


/**************************************************************************************************/
/*                                                                                                */
/*   Optional_Checks_Code macro compartmentalizes various checks that can be run at any time      */
/*   Check #1 - Storage monitor: LoanMaster file size vs the 5 GB limit                           */
/*   Check #2 - ETL Pipeline verification: processing record                                      */
/*   Check #3 - ETL Pipeline verification: grand totals                                           */
/*   Check #4 - WORK reset: empties the WORK scratch library. Starred - un-star only after        */
/*              a failed run                                                                      */
/*   Run any check at any time to validate successful code execution                              */
/*   Last step, run %Optional_Checks_Code; at the end of ETL processing to verify success         */
/*                                                                                                */
/**************************************************************************************************/

%MACRO Optional_Checks_Code;

    /*   Storage monitor - Code to check loanmaster filesize   */
    /*   Watch LoanMaster vs the 5 GB limit                      */
    PROC SQL;
        TITLE "FNMAE Storage by Dataset";
        SELECT Memname, Filesize FORMAT=SIZEKMG12.1, Nobs FORMAT=COMMA14.
        FROM DICTIONARY.Tables
        WHERE Libname = 'FNMAE'
        ORDER BY Filesize DESC;
    QUIT;

    /*   ETL Pipeline verification - processing record    */
    PROC PRINT DATA=FNMAE.RunLog NOOBS LABEL;
        TITLE "ETL Pipeline Processing Record";
    RUN;

    /*   ETL Pipeline verification - grand totals   */
    PROC SQL;
        TITLE "ETL Pipeline: Grand Totals";
        SELECT count(*)                        LABEL="Data sets processed",
               sum(RawRows)                    LABEL="Total monthly records"      FORMAT=COMMA18.,
               sum(LoanRows)                   LABEL="Total loans"               FORMAT=COMMA18.,
               sum(Defaults)                    LABEL="Total defaults"            FORMAT=COMMA18.,
               sum(ProcessTime)                LABEL="Total SAS processing time"  FORMAT=TIME12.,
               sum(RawRows) / sum(ProcessTime) LABEL="Records per second"         FORMAT=COMMA12.
        FROM FNMAE.RunLog;
    QUIT;

    TITLE;

    /*   WORK reset - empties the WORK library. Un-star only after a failed run   */
    *PROC DATASETS LIBRARY=WORK KILL NOLIST;
    *QUIT;

%MEND Optional_Checks_Code;

%Optional_Checks_Code;



/**************************************************************************************************/
/*                                                                                                */
/*   Generate_Pdf macro compartmentalizes the code that produces the pipeline report PDF          */
/*   Page 1 - title and pipeline overview                                                         */
/*   Page 2 - processing record, grand totals, and records by acquisition quarter                 */
/*   Last step, run %Generate_Pdf; after all processing is complete and verified                  */
/*                                                                                                */
/**************************************************************************************************/

%MACRO Generate_Pdf;

    OPTIONS NODATE NONUMBER ORIENTATION=PORTRAIT
            TOPMARGIN=1IN BOTTOMMARGIN=1IN LEFTMARGIN=1IN RIGHTMARGIN=1IN;
    ODS _ALL_ CLOSE;
    ODS GRAPHICS ON / WIDTH=6.4IN HEIGHT=2.6IN OUTPUTFMT=PNG;
    ODS ESCAPECHAR='^';

    ODS PDF FILE="&root/EtlPipeline_Report.pdf" STYLE=PEARL STARTPAGE=NO;

    ODS PDF TEXT="^{newline 2}";
    ODS PDF TEXT="^{style [FONTSIZE=26PT FONTWEIGHT=BOLD COLOR=CX1F4E79 JUST=C] ETL Pipeline}";
    ODS PDF TEXT="^{style [FONTSIZE=17PT FONTWEIGHT=BOLD COLOR=CX1F4E79 JUST=C] ~1.2 Billion Records Processed in a 5 GB Environment}";
    ODS PDF TEXT="^{newline 2}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT JUST=C] Fannie Mae Single-Family Loan Performance Data}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT JUST=C] Anne M. Prihoda}";
    ODS PDF TEXT="^{newline 3}";

    ODS PDF TEXT="^{style [FONTSIZE=14PT FONTWEIGHT=BOLD COLOR=CX1F4E79] What the pipeline does}";
    ODS PDF TEXT="^{newline}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT]    -  Processes 48 Fannie Mae quarterly loan performance files - over one billion pipe-delimited monthly records}";
    ODS PDF TEXT="^{newline}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT]    -  Operates inside a 5 GB SAS OnDemand environment with a 1 GB per-file upload limit}";
    ODS PDF TEXT="^{newline}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT]    -  Quarters exceeding the 1 GB upload limit are pre-split on loan boundaries by a PowerShell utility: Cutting only between loans keeps loan history intact}";
    ODS PDF TEXT="^{newline}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT]    -  Source files are read directly from gzip through the FILENAME ZIP engine: Enables 35 GB of raw text to be processed within the 5 GB storage limit}";
    ODS PDF TEXT="^{newline}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT]    -  Retains only the fields the scorecard requires, applies informats to convert MMYYYY dates and coded text, and derives the modelling fields: minimum credit score, origination quarter, and the default flag}";
    ODS PDF TEXT="^{newline}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT]    -  Collapses each loan history of monthly records into a single analytical row carrying its origination characteristics and final outcome, then appends it to the master table}";
    ODS PDF TEXT="^{newline}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT]    -  Runs idempotently - reprocessing a file replaces its prior load, never duplicates it}";
    ODS PDF TEXT="^{newline}";
    ODS PDF TEXT="^{style [FONTSIZE=13PT]    -  Records the counts for every file in the processing record, which reconciles to Fannie Mae published statistical summaries}";
    ODS PDF STARTPAGE=NOW;

    TITLE1 "ETL Pipeline Processing Record";
    PROC PRINT DATA=FNMAE.RunLog NOOBS LABEL;
    RUN;

    TITLE1 "ETL Pipeline: Grand Totals";
    PROC SQL;
        SELECT count(*)                        LABEL="Data sets processed",
               sum(RawRows)                    LABEL="Total monthly records"      FORMAT=COMMA18.,
               sum(LoanRows)                   LABEL="Total loans"               FORMAT=COMMA14.,
               sum(Defaults)                    LABEL="Total defaults"            FORMAT=COMMA12.,
               sum(ProcessTime)                LABEL="Total SAS processing time"  FORMAT=TIME12.,
               sum(RawRows) / sum(ProcessTime) LABEL="Records per second"         FORMAT=COMMA12.
        FROM FNMAE.RunLog;
    QUIT;

    TITLE2 "Monthly records read by acquisition quarter";
    PROC SGPLOT DATA=FNMAE.RunLog;
        VBAR Quarter / RESPONSE=RawRows STAT=SUM FILLATTRS=(COLOR=CX1F4E79);
        YAXIS LABEL="Monthly records" GRID;
        XAXIS LABEL="Acquisition quarter" FITPOLICY=THIN;
    RUN;

    TITLE;
    ODS PDF CLOSE;
    ODS GRAPHICS OFF;

%MEND Generate_Pdf;

%Generate_Pdf;



/* release the log post project completion */
*PROC PRINTTO; 
*RUN;







