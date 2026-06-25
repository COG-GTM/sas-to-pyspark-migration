/*******************************************************************************/
/*  SAS Program: 06_scorecard_scoring.sas                                    */
/*  Purpose: Apply a points-based credit scorecard and make lending          */
/*           decisions for the home-equity portfolio.                        */
/*  - PROC FORMAT to bucket continuous predictors                            */
/*  - DATA step scorecard: convert characteristics to points                 */
/*  - Decision logic (Approve / Refer / Decline)                             */
/*  - PROC SQL portfolio KPIs + PROC MEANS/FREQ summaries                    */
/*  - Persist the scored output for downstream reporting                     */
/*                                                                            */
/*  Input : work.home_equity_risk  (produced by 04_risk_segmentation.sas)    */
/*  Output: work.scorecard_scored                                            */
/*******************************************************************************/

/* Step 1: Formats used to bucket characteristics into scorecard bands */
proc format;
    value ltvband
        low - 0.6   = 'A: <=60%'
        0.6 <- 0.8  = 'B: 60-80%'
        0.8 <- 0.9  = 'C: 80-90%'
        0.9 <- high = 'D: >90%';
    value dtiband
        low - 30    = 'A: <=30'
        30 <- 40    = 'B: 30-40'
        40 <- high  = 'C: >40';
    value clageband
        low - 60    = 'A: <5yr'
        60 <- 180   = 'B: 5-15yr'
        180 <- high = 'C: >15yr';
run;

/* Step 2: Build the scorecard. Each characteristic contributes points; the   */
/*         total is scaled to a 300-850 style score, then bucketed to a grade. */
data work.scorecard_scored;
    set work.home_equity_risk;

    /* Base points */
    points = 500;

    /* Loan-to-value points (lower LTV is safer) */
    if      LTV <= 0.6 then points = points + 60;
    else if LTV <= 0.8 then points = points + 30;
    else if LTV <= 0.9 then points = points - 10;
    else                    points = points - 60;

    /* Debt-to-income points */
    if      DEBTINC = .   then points = points - 20; /* missing DTI penalty */
    else if DEBTINC <= 30 then points = points + 50;
    else if DEBTINC <= 40 then points = points + 10;
    else                       points = points - 50;

    /* Delinquency and derogatory history */
    points = points - (DELINQ * 25);
    points = points - (DEROG  * 40);

    /* Recent credit inquiries */
    if NINQ > 2 then points = points - 30;

    /* Age of oldest credit line (months) */
    if      CLAGE >= 180 then points = points + 30;
    else if CLAGE >= 60  then points = points + 10;

    /* Employment stability */
    if YOJ >= 10 then points = points + 20;

    /* Clamp to a 300-850 range */
    SCORE = min(max(points, 300), 850);

    /* Letter grade from score */
    length CREDIT_GRADE $1;
    if      SCORE >= 720 then CREDIT_GRADE = 'A';
    else if SCORE >= 660 then CREDIT_GRADE = 'B';
    else if SCORE >= 600 then CREDIT_GRADE = 'C';
    else                      CREDIT_GRADE = 'D';

    /* Decision logic: combine grade with composite risk segment */
    length DECISION $8;
    if CREDIT_GRADE in ('A', 'B') and RISK_SEGMENT ne 'Very High Risk'
        then DECISION = 'Approve';
    else if CREDIT_GRADE = 'C'
        then DECISION = 'Refer';
    else DECISION = 'Decline';

    LTV_BAND   = put(LTV, ltvband.);
    DTI_BAND   = put(DEBTINC, dtiband.);
    CLAGE_BAND = put(CLAGE, clageband.);

    label SCORE        = "Credit Score (300-850)"
          CREDIT_GRADE = "Credit Grade"
          DECISION     = "Lending Decision"
          LTV_BAND     = "LTV Band"
          DTI_BAND     = "DTI Band"
          CLAGE_BAND   = "Credit Age Band";
run;

/* Step 3: Portfolio KPIs by decision (PROC SQL) */
title "Portfolio KPIs by Lending Decision";
proc sql;
    create table work.decision_kpis as
    select DECISION,
           count(*)                       as n_loans,
           sum(LOAN)                       as total_exposure  format=dollar15.2,
           mean(SCORE)                     as avg_score       format=6.1,
           mean(BAD)                       as default_rate    format=percent8.2
    from work.scorecard_scored
    group by DECISION
    order by calculated default_rate desc;
quit;

proc print data=work.decision_kpis noobs label;
run;
title;

/* Step 4: Score distribution and observed default rate by grade */
title "Score and Default Rate by Credit Grade";
proc means data=work.scorecard_scored n mean std min p25 median p75 max maxdec=2;
    class CREDIT_GRADE;
    var SCORE BAD;
run;
title;

/* Step 5: Cross-tab of grade vs decision */
title "Credit Grade by Lending Decision";
proc freq data=work.scorecard_scored;
    tables CREDIT_GRADE * DECISION / nocum nopercent norow nocol;
run;
title;

/* Step 6: Persist the scored portfolio for downstream consumption */
proc export data=work.scorecard_scored
    outfile="scorecard_scored.csv"
    dbms=csv
    replace;
run;
