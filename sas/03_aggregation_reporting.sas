/*******************************************************************************/
/*  SAS Program: 03_aggregation_reporting.sas                                */
/*  Purpose: Generate frequency tables, summary statistics, and reports      */
/*  - PROC FREQ for categorical distributions                                */
/*  - PROC MEANS for numeric summaries by group                              */
/*  - PROC TABULATE for cross-tabulations                                    */
/*  - PROC SQL for ad hoc queries                                            */
/*******************************************************************************/

/* Step 1: Frequency tables for categorical variables */
title "Frequency Tables: Job Category, Loan Reason, Loan Outcome, Region";
proc freq data=work.home_equity_final;
    tables JOB REASON LOAN_OUTCOME REGION / nocum;
run;
title;

/* Step 2: Summary statistics by loan outcome */
title "Summary Statistics by Loan Outcome";
proc means data=work.home_equity_final
    n mean median std min max;
    class LOAN_OUTCOME;
    var LOAN MORTDUE VALUE DEBTINC;
run;
title;

/* Step 3: Cross-tabulation of default rates by JOB and REGION */
title "Default Rates by Job Category and Region";
proc tabulate data=work.home_equity_final;
    class JOB REGION;
    var BAD;
    table JOB all='Total',
          REGION * BAD * (n mean*f=percent8.2) all='Total' * BAD * (n mean*f=percent8.2);
run;
title;

/* Step 4: Top 10 states by average loan amount using PROC SQL */
title "Top 10 States by Average Loan Amount";
proc sql outobs=10;
    select STATE,
           count(*) as num_loans,
           mean(LOAN) as avg_loan format=dollar12.,
           mean(VALUE) as avg_property_value format=dollar12.,
           mean(BAD) as default_rate format=percent8.2
    from work.home_equity_final
    group by STATE
    having count(*) >= 10
    order by avg_loan desc;
quit;
title;

/* Step 5: Additional reporting - Loan distribution by reason and outcome */
title "Loan Amount Distribution by Reason and Outcome";
proc means data=work.home_equity_final n mean std median;
    class REASON LOAN_OUTCOME;
    var LOAN LTV DEBTINC;
run;
title;
