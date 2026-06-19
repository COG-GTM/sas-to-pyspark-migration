/*******************************************************************************/
/*  SAS Program: 06_model_scoring.sas                                       */
/*  Purpose: Model evaluation and scoring pipeline                          */
/*  - Score new/holdout data using a trained logistic regression model       */
/*  - Compute evaluation metrics (AUC, KS, Gini, accuracy)                  */
/*  - Generate decile-based lift chart analysis                              */
/*  - Produce a gains table and model scorecard                              */
/*******************************************************************************/

/* Step 1: Prepare the modeling dataset (same filters as Stage 5) */
data work.model_data;
    set work.home_equity_risk;
    if LOAN ne . and MORTDUE ne . and VALUE ne .
       and DEBTINC ne . and DELINQ ne . and CLAGE ne .
       and DEROG ne . and NINQ ne .
       and JOB ne '' and REASON ne '';
run;

/* Step 2: Split into train (60%), validation (20%), holdout (20%) */
proc surveyselect data=work.model_data
    out=work.model_split
    method=srs
    samprate=0.6
    seed=42;
run;

data work.train work.remaining;
    set work.model_split;
    if selected = 1 then output work.train;
    else output work.remaining;
run;

proc surveyselect data=work.remaining
    out=work.val_split
    method=srs
    samprate=0.5
    seed=42;
run;

data work.valid work.holdout;
    set work.val_split;
    if selected = 1 then output work.valid;
    else output work.holdout;
run;

/* Step 3: Fit logistic regression on training data and store model */
title "Stage 6 - Logistic Regression Model for Scoring Pipeline";
proc logistic data=work.train descending;
    class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
    model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
                JOB REASON;
    output out=work.train_scored predicted=pred_prob;
    store work.scoring_model;
run;
title;

/* Step 4: Score validation and holdout datasets */
proc plm restore=work.scoring_model;
    score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
run;

proc plm restore=work.scoring_model;
    score data=work.holdout out=work.holdout_scored predicted=pred_prob / ilink;
run;

/* Step 5: Assign predicted classes using 0.5 threshold */
%macro assign_classes(dsn);
    data &dsn.;
        set &dsn.;
        if pred_prob >= 0.5 then PREDICTED_BAD = 1;
        else PREDICTED_BAD = 0;
    run;
%mend assign_classes;

%assign_classes(work.valid_scored);
%assign_classes(work.holdout_scored);

/* Step 6: Confusion matrix on holdout */
title "Confusion Matrix - Holdout Set";
proc freq data=work.holdout_scored;
    tables BAD * PREDICTED_BAD / nopercent norow nocol;
run;
title;

/* Step 7: Compute accuracy, precision, recall on holdout */
proc sql;
    create table work.holdout_metrics as
    select
        count(*) as N,
        sum(case when BAD = PREDICTED_BAD then 1 else 0 end) / count(*) as Accuracy,
        sum(case when BAD = 1 and PREDICTED_BAD = 1 then 1 else 0 end)
            / max(sum(case when PREDICTED_BAD = 1 then 1 else 0 end), 1) as Precision,
        sum(case when BAD = 1 and PREDICTED_BAD = 1 then 1 else 0 end)
            / max(sum(case when BAD = 1 then 1 else 0 end), 1) as Recall
    from work.holdout_scored;
quit;

title "Holdout Set - Classification Metrics";
proc print data=work.holdout_metrics noobs;
    format Accuracy Precision Recall percent8.2;
run;
title;

/* Step 8: Decile analysis / lift chart */
proc rank data=work.holdout_scored out=work.holdout_deciles
    groups=10 descending;
    var pred_prob;
    ranks decile;
run;

proc sql;
    create table work.gains_table as
    select
        decile + 1 as Decile,
        count(*) as N,
        sum(BAD) as Defaults,
        sum(BAD) / count(*) as Default_Rate format=percent8.2,
        mean(pred_prob) as Avg_Score format=8.4,
        min(pred_prob) as Min_Score format=8.4,
        max(pred_prob) as Max_Score format=8.4
    from work.holdout_deciles
    group by decile
    order by decile;
quit;

title "Gains Table - Decile Analysis";
proc print data=work.gains_table noobs;
run;
title;

/* Step 9: Cumulative gains and KS statistic */
proc sql;
    create table work.cumulative_gains as
    select
        Decile,
        N,
        Defaults,
        Default_Rate,
        Avg_Score,
        sum(Defaults) as Cum_Defaults,
        sum(N) as Cum_N,
        calculated Cum_Defaults / (select sum(BAD) from work.holdout_scored) as Cum_Default_Pct format=percent8.2,
        calculated Cum_N / (select count(*) from work.holdout_scored) as Cum_Pop_Pct format=percent8.2,
        calculated Cum_Default_Pct - calculated Cum_Pop_Pct as KS_Diff format=percent8.2
    from work.gains_table
    order by Decile;
quit;

title "Cumulative Gains and KS Analysis";
proc print data=work.cumulative_gains noobs;
run;
title;

/* Step 10: AUC on holdout via PROC LOGISTIC with ROC */
title "ROC Analysis - Holdout Set";
proc logistic data=work.holdout descending;
    class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
    model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
                JOB REASON;
    roc;
    ods output Association=work.holdout_assoc;
run;
title;

/* Step 11: Model scorecard - assign risk grades based on predicted probability */
data work.scorecard;
    set work.holdout_scored;
    length RISK_GRADE $12;
    if pred_prob < 0.10 then RISK_GRADE = 'A - Minimal';
    else if pred_prob < 0.25 then RISK_GRADE = 'B - Low';
    else if pred_prob < 0.50 then RISK_GRADE = 'C - Moderate';
    else if pred_prob < 0.75 then RISK_GRADE = 'D - High';
    else RISK_GRADE = 'E - Critical';

    label RISK_GRADE = "Model Risk Grade"
          pred_prob = "Predicted Default Probability";
run;

title "Risk Grade Distribution";
proc freq data=work.scorecard;
    tables RISK_GRADE / nocum;
run;
title;

title "Default Rate by Risk Grade";
proc means data=work.scorecard n mean std;
    class RISK_GRADE;
    var BAD pred_prob LOAN DEBTINC;
run;
title;

/* Step 12: Summary statistics on scored output */
title "Predicted Probability Distribution by Actual Outcome";
proc means data=work.holdout_scored n mean std min p25 median p75 max;
    class BAD;
    var pred_prob;
run;
title;
