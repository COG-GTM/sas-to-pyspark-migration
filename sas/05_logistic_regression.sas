/*******************************************************************************/
/*  SAS Program: 05_logistic_regression.sas                                  */
/*  Purpose: Build a logistic regression model for loan default prediction   */
/*  - PROC LOGISTIC with stepwise selection                                  */
/*  - Model evaluation (concordance, AUC)                                    */
/*  - Predicted probabilities and confusion matrix                           */
/*******************************************************************************/

/* Step 1: Prepare modeling dataset - remove records with excessive missing */
data work.model_data;
    set work.home_equity_risk;
    /* Keep only complete cases for key predictors */
    if LOAN ne . and MORTDUE ne . and VALUE ne .
       and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
run;

/* Step 2: Split into training (70%) and validation (30%) */
proc surveyselect data=work.model_data
    out=work.model_split
    method=srs
    samprate=0.7
    seed=42;
run;

data work.train work.valid;
    set work.model_split;
    if selected = 1 then output work.train;
    else output work.valid;
run;

/* Step 3: Fit logistic regression with stepwise selection */
title "Logistic Regression: Loan Default Prediction";
proc logistic data=work.train descending;
    class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
    model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
                JOB REASON
              / selection=stepwise
                slentry=0.05
                slstay=0.05
                details
                lackfit;
    output out=work.train_scored predicted=pred_prob;
    store work.logit_model;
run;
title;

/* Step 4: Score the validation dataset */
proc plm restore=work.logit_model;
    score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
run;

/* Step 5: Create predicted classes and confusion matrix */
data work.valid_scored;
    set work.valid_scored;
    if pred_prob >= 0.5 then PREDICTED_BAD = 1;
    else PREDICTED_BAD = 0;
run;

title "Confusion Matrix - Validation Set";
proc freq data=work.valid_scored;
    tables BAD * PREDICTED_BAD / nopercent norow nocol;
run;
title;

/* Step 6: Calculate model performance metrics */
title "Model Performance - Concordance and AUC";
proc logistic data=work.valid descending;
    class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
    model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
                JOB REASON;
    roc;
    ods output Association=work.association_stats;
run;
title;

/* Step 7: Display key metrics */
title "Model Association Statistics";
proc print data=work.association_stats;
run;
title;

/* Step 8: Score distribution by actual outcome */
title "Predicted Probability Distribution by Actual Outcome";
proc means data=work.valid_scored n mean std min p25 median p75 max;
    class BAD;
    var pred_prob;
run;
title;
