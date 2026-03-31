/*******************************************************************************/
/*  SAS Program: 02_data_cleaning.sas                                        */
/*  Purpose: Clean and transform the HOME_EQUITY dataset                     */
/*  - Create derived columns (LTV, LOAN_OUTCOME)                            */
/*  - Handle missing values                                                  */
/*  - Filter records and check for outliers                                  */
/*******************************************************************************/

/* Step 1: Create derived columns using DATA step */
data work.home_equity_clean;
    length LOAN_OUTCOME $ 7;
    set work.home_equity;

    /* Calculate Loan-to-Value ratio */
    if VALUE ne . and MORTDUE ne . and VALUE > 0 then
        LTV = MORTDUE / VALUE;
    else
        LTV = .;

    /* Create descriptive loan outcome */
    if BAD = 0 then LOAN_OUTCOME = 'Paid';
    else if BAD = 1 then LOAN_OUTCOME = 'Default';
    else LOAN_OUTCOME = '';

    /* Proper-case city names */
    CITY = propcase(CITY);

    /* Apply labels */
    label LTV = "Loan to Value Ratio"
          LOAN_OUTCOME = "Loan Outcome";
    format LTV percent8.2;
run;

/* Step 2: Handle missing values using array processing */
data work.home_equity_imputed;
    set work.home_equity_clean;

    /* Array of numeric variables to impute */
    array num_vars{8} LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ;
    array num_flags{8} LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS
                       DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;

    /* Flag missing values before imputation */
    do i = 1 to 8;
        if num_vars{i} = . then num_flags{i} = 1;
        else num_flags{i} = 0;
    end;

    drop i;
run;

/* Step 3: Filter out records with missing critical fields */
data work.home_equity_filtered;
    set work.home_equity_imputed;
    /* Keep only records where LOAN, VALUE, and BAD are non-missing */
    if LOAN ne . and VALUE ne . and BAD ne .;
run;

/* Step 4: Check for outliers using PROC MEANS */
title "Summary Statistics for Outlier Detection";
proc means data=work.home_equity_filtered
    n nmiss mean std min p1 p5 p25 median p75 p95 p99 max;
    var LOAN MORTDUE VALUE DEBTINC LTV CLAGE DEROG DELINQ;
run;
title;

/* Step 5: Create the final clean dataset */
data work.home_equity_final;
    set work.home_equity_filtered;
    /* Remove extreme outliers */
    if LTV > 0 and LTV < 5;          /* LTV ratio sanity check */
    if LOAN > 0;                       /* Positive loan amounts only */
    if VALUE > 0;                      /* Positive property values only */
run;

title "Clean Dataset Summary";
proc means data=work.home_equity_final n nmiss mean std min max;
    var LOAN MORTDUE VALUE LTV DEBTINC;
run;
title;

proc print data=work.home_equity_final(obs=10);
    var BAD LOAN MORTDUE VALUE LTV LOAN_OUTCOME DEBTINC JOB REASON;
run;
