/*******************************************************************************/
/*  SAS Program: 04_risk_segmentation.sas                                    */
/*  Purpose: Create risk segments for loan portfolio analysis                */
/*  - Define risk buckets using PROC FORMAT                                  */
/*  - Create composite risk scores                                           */
/*  - Analyze default rates by risk segment                                  */
/*******************************************************************************/

/* Step 1: Define risk bucket formats using PROC FORMAT */
proc format;
    /* LTV Risk Categories */
    value ltv_risk
        low  -< 0.60 = 'Low'
        0.60 -< 0.80 = 'Medium'
        0.80 - high   = 'High';

    /* Debt-to-Income Risk Categories */
    value dti_risk
        low  -< 30  = 'Low'
        30   -< 40  = 'Medium'
        40   -< 50  = 'High'
        50   - high  = 'Very High';

    /* Delinquency Risk Categories */
    value delinq_risk
        0         = 'None'
        1         = 'Low'
        2 - 3     = 'Medium'
        4 - high  = 'High';

    /* Combined Risk Score Categories */
    value risk_score
        low  -< 3 = 'Low Risk'
        3    -< 5 = 'Medium Risk'
        5    -< 7 = 'High Risk'
        7 - high  = 'Very High Risk';
run;

/* Step 2: Create risk segment variables */
data work.home_equity_risk;
    set work.home_equity_final;

    /* LTV risk category */
    length LTV_RISK_CAT $6 DTI_RISK_CAT $9 DELINQ_RISK_CAT $6;

    if LTV ne . then do;
        if LTV < 0.60 then LTV_RISK_CAT = 'Low';
        else if LTV < 0.80 then LTV_RISK_CAT = 'Medium';
        else LTV_RISK_CAT = 'High';
    end;

    /* Debt-to-Income risk category */
    if DEBTINC ne . then do;
        if DEBTINC < 30 then DTI_RISK_CAT = 'Low';
        else if DEBTINC < 40 then DTI_RISK_CAT = 'Medium';
        else if DEBTINC < 50 then DTI_RISK_CAT = 'High';
        else DTI_RISK_CAT = 'Very High';
    end;

    /* Delinquency risk category */
    if DELINQ ne . then do;
        if DELINQ = 0 then DELINQ_RISK_CAT = 'None';
        else if DELINQ = 1 then DELINQ_RISK_CAT = 'Low';
        else if DELINQ <= 3 then DELINQ_RISK_CAT = 'Medium';
        else DELINQ_RISK_CAT = 'High';
    end;

    /* Composite risk score (0-10 scale) */
    RISK_SCORE = 0;

    /* LTV component (0-3 points) */
    if LTV ne . then do;
        if LTV >= 0.80 then RISK_SCORE = RISK_SCORE + 3;
        else if LTV >= 0.60 then RISK_SCORE = RISK_SCORE + 1.5;
    end;

    /* DTI component (0-3 points) */
    if DEBTINC ne . then do;
        if DEBTINC >= 50 then RISK_SCORE = RISK_SCORE + 3;
        else if DEBTINC >= 40 then RISK_SCORE = RISK_SCORE + 2;
        else if DEBTINC >= 30 then RISK_SCORE = RISK_SCORE + 1;
    end;

    /* Delinquency component (0-2 points) */
    if DELINQ ne . then do;
        if DELINQ >= 4 then RISK_SCORE = RISK_SCORE + 2;
        else if DELINQ >= 2 then RISK_SCORE = RISK_SCORE + 1.5;
        else if DELINQ = 1 then RISK_SCORE = RISK_SCORE + 0.5;
    end;

    /* Derogatory reports component (0-2 points) */
    if DEROG ne . then do;
        if DEROG >= 3 then RISK_SCORE = RISK_SCORE + 2;
        else if DEROG >= 1 then RISK_SCORE = RISK_SCORE + 1;
    end;

    /* Risk score category */
    length RISK_SEGMENT $14;
    if RISK_SCORE < 3 then RISK_SEGMENT = 'Low Risk';
    else if RISK_SCORE < 5 then RISK_SEGMENT = 'Medium Risk';
    else if RISK_SCORE < 7 then RISK_SEGMENT = 'High Risk';
    else RISK_SEGMENT = 'Very High Risk';

    label LTV_RISK_CAT = "LTV Risk Category"
          DTI_RISK_CAT = "Debt-to-Income Risk Category"
          DELINQ_RISK_CAT = "Delinquency Risk Category"
          RISK_SCORE = "Composite Risk Score (0-10)"
          RISK_SEGMENT = "Risk Segment";
run;

/* Step 3: Distribution across risk segments */
title "Distribution of Loans by Risk Segment";
proc freq data=work.home_equity_risk;
    tables RISK_SEGMENT LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT / nocum;
run;
title;

/* Step 4: Default rate by risk segment */
title "Average Default Rate by Risk Segment";
proc means data=work.home_equity_risk n mean std;
    class RISK_SEGMENT;
    var BAD LOAN LTV DEBTINC;
run;
title;

/* Step 5: Cross-tabulation of risk segments */
title "Default Rates by LTV Risk and DTI Risk";
proc freq data=work.home_equity_risk;
    tables LTV_RISK_CAT * DTI_RISK_CAT * BAD / norow nocol nopercent;
run;
title;
