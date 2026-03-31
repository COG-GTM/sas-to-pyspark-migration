/*******************************************************************************/
/*  SAS Program: 01_data_loading.sas                                         */
/*  Purpose: Load and explore the HOME_EQUITY dataset                        */
/*  Dataset: home_equity.csv - Home equity loan data for risk analysis       */
/*******************************************************************************/

/* Step 1: Import CSV data into SAS dataset */
proc import datafile="/data/home_equity.csv"
    dbms=csv
    out=work.home_equity
    replace;
    guessingrows=5960;
run;

/* Step 2: Apply variable labels for documentation and reporting */
proc datasets lib=work memtype=data nolist;
    modify home_equity;
    label BAD="Loan Status (1=Default, 0=Paid)"
          LOAN="Amount of Loan Request"
          MORTDUE="Amount Due on Existing Mortgage"
          VALUE="Value of Current Property"
          REASON="Loan Purpose (HomeImp or DebtCon)"
          JOB="Job Category"
          YOJ="Years at Present Job"
          DEROG="Number of Derogatory Reports"
          DELINQ="Number of Delinquent Credit Lines"
          CLAGE="Age of Oldest Credit Line (months)"
          NINQ="Number of Recent Credit Inquiries"
          CLNO="Number of Credit Lines"
          DEBTINC="Debt to Income Ratio"
          APPDATE="Loan Application Date"
          CITY="City"
          STATE="State"
          DIVISION="Census Division"
          REGION="Census Region";
quit;

/* Step 3: Apply formats for display */
proc datasets lib=work memtype=data nolist;
    modify home_equity;
    format LOAN MORTDUE VALUE dollar12.
           APPDATE date9.
           DEBTINC 8.1
           CLAGE comma8.1;
quit;

/* Step 4: Display dataset metadata (column names, types, formats) */
title "HOME_EQUITY Dataset Metadata";
proc contents data=work.home_equity;
run;
title;

/* Step 5: Preview first 20 observations */
title "First 20 Observations of HOME_EQUITY";
proc print data=work.home_equity(obs=20);
run;
title;
