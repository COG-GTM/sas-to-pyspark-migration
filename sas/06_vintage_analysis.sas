/* vintage_analysis.sas — monthly cohort / vintage report            */
/* Author: credit risk analytics                                       */

%let thresholds = 1 2 3;

proc import datafile="/data/home_equity.csv" dbms=csv out=work.he replace;
    guessingrows=max;
run;

/* derive cohort & clean-up */
data work.he2;
    set work.he;
    length COHORT $7 REASON_CLEAN $7;
    if missing(REASON) then REASON_CLEAN = 'UNKNOWN';
    else REASON_CLEAN = upcase(REASON);
    /* treat missing YOJ as zero years on job */
    if YOJ = . then YOJ = 0;
    /* cohort = years-on-job bucket, stands in for origination vintage */
    if YOJ < 2 then COHORT = 'V0-2';
    else if YOJ < 5 then COHORT = 'V2-5';
    else if YOJ < 10 then COHORT = 'V5-10';
    else COHORT = 'V10+';
    LTV = MORTDUE / VALUE;
    if VALUE in (., 0) then LTV = .;
run;

/* running exposure & first/last flags within cohort, ordered by LOAN */
proc sort data=work.he2; by COHORT LOAN; run;

data work.he3;
    set work.he2;
    by COHORT;
    retain CUM_LOAN 0 SEQ 0;
    if first.COHORT then do; CUM_LOAN = 0; SEQ = 0; end;
    CUM_LOAN + LOAN;
    SEQ + 1;
    IS_FIRST = first.COHORT;
    IS_LAST  = last.COHORT;
    if last.COHORT then output;
    /* only the last row per cohort carries the cohort total */
run;

/* cohort summary */
proc sql;
    create table work.cohort_summary as
    select COHORT,
           count(*)                       as N_LOANS,
           sum(BAD)                       as N_BAD,
           calculated N_BAD / calculated N_LOANS as DEFAULT_RATE format=percent8.2,
           mean(LOAN)                     as AVG_LOAN format=dollar12.,
           mean(LTV)                      as AVG_LTV format=8.3,
           mean(DEBTINC)                  as AVG_DTI format=8.2
    from work.he2
    group by COHORT
    order by COHORT;
quit;

/* delinquency threshold sweep */
%macro delinq_sweep;
    %local i thr;
    %do i = 1 %to %sysfunc(countw(&thresholds));
        %let thr = %scan(&thresholds, &i);
        proc sql;
            create table work.delinq_ge_&thr as
            select COHORT,
                   &thr                                as THRESHOLD,
                   sum(DELINQ >= &thr)                 as N_OVER,
                   count(*)                            as N,
                   calculated N_OVER / calculated N    as PCT_OVER format=percent8.2
            from work.he2
            where not missing(DELINQ)
            group by COHORT;
        quit;
    %end;
%mend;
%delinq_sweep;

data work.delinq_all;
    set work.delinq_ge_1 work.delinq_ge_2 work.delinq_ge_3;
run;

proc print data=work.cohort_summary noobs label; run;
proc print data=work.delinq_all noobs; run;
