/*******************************************************************************/
/*  SAS Program: 06_simple_ltv_bands.sas                                     */
/*  Purpose: Minimal end-to-end migration example                            */
/*  - Read HOME_EQUITY                                                       */
/*  - Keep rows with a usable LOAN and VALUE                                 */
/*  - Derive LTV and an LTV_BAND (LOW / MEDIUM / HIGH / UNKNOWN)              */
/*  - Summarise loans and default rate per band with PROC MEANS              */
/*******************************************************************************/

proc import datafile="data/home_equity.csv"
    out=work.he06_input
    dbms=csv
    replace;
    getnames=yes;
run;

/* Step 1: DATA step - filter, derive LTV and band each row */
data work.ltv_bands;
    length LTV_BAND $ 7;
    set work.he06_input(keep=BAD LOAN MORTDUE VALUE REASON);

    /* Subsetting IF: drop rows with no loan amount or property value */
    if LOAN ne . and VALUE ne . and VALUE > 0;

    if MORTDUE ne . then LTV = MORTDUE / VALUE;
    else LTV = .;

    /* Missing (.) sorts below every number in SAS, so test for it first */
    if LTV = .         then LTV_BAND = 'UNKNOWN';
    else if LTV < 0.8  then LTV_BAND = 'LOW';
    else if LTV < 1.0  then LTV_BAND = 'MEDIUM';
    else                    LTV_BAND = 'HIGH';
run;

/* Step 2: PROC MEANS - one output row per LTV_BAND */
proc means data=work.ltv_bands noprint nway;
    class LTV_BAND;
    var LOAN BAD;
    output out=work.ltv_band_summary(drop=_TYPE_ _FREQ_)
        n(LOAN)    = N_LOANS
        mean(LOAN) = AVG_LOAN
        mean(BAD)  = DEFAULT_RATE;
run;

/* Step 3: Sort and print the result */
proc sort data=work.ltv_band_summary;
    by LTV_BAND;
run;

title "Loans and Default Rate by LTV Band";
proc print data=work.ltv_band_summary noobs;
    format AVG_LOAN comma12.2 DEFAULT_RATE percent8.2;
run;
title;
