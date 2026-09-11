# Parity results: 04_risk_segmentation

Log checked: `validation/04_risk_segmentation/logs/run_before_fixes.txt`

SAS row base (work.home_equity_final): **5337** rows (LOAN/VALUE/BAD non-missing, 0 < LTV < 5, LOAN > 0, VALUE > 0).

## Summary: 33 / 117 checks matched

| Section | Check | Expected (SAS semantics) | Actual (PySpark log) | Status | Detail |
|---|---|---|---|---|---|
| RISK_SEGMENT | Frequency[Low Risk] | 3118 | 3603 | MISMATCH | diff=485 |
| RISK_SEGMENT | Percent[Low Risk] (base excludes missing) | 58.42 | 61.61 | MISMATCH | diff=3.19 |
| RISK_SEGMENT | Frequency[Medium Risk] | 1776 | 1784 | MISMATCH | diff=8 |
| RISK_SEGMENT | Percent[Medium Risk] (base excludes missing) | 33.28 | 30.51 | MISMATCH | diff=2.77 |
| RISK_SEGMENT | Frequency[High Risk] | 427 | 445 | MISMATCH | diff=18 |
| RISK_SEGMENT | Percent[High Risk] (base excludes missing) | 8 | 7.61 | MISMATCH | diff=0.39 |
| RISK_SEGMENT | Frequency[Very High Risk] | 16 | 16 | MATCH | diff=0 |
| RISK_SEGMENT | Percent[Very High Risk] (base excludes missing) | 0.3 | 0.27 | MISMATCH | diff=0.03 |
| RISK_SEGMENT | table excludes missing level (PROC FREQ default) | excluded | excluded | MATCH |  |
| RISK_SEGMENT | Frequency Missing | 0 | - | MISMATCH | not printed |
| LTV_RISK_CAT | Frequency[Low] | 1131 | 1131 | MATCH | diff=0 |
| LTV_RISK_CAT | Percent[Low] (base excludes missing) | 21.19 | 19.34 | MISMATCH | diff=1.85 |
| LTV_RISK_CAT | Frequency[Medium] | 2967 | 2967 | MATCH | diff=0 |
| LTV_RISK_CAT | Percent[Medium] (base excludes missing) | 55.59 | 50.74 | MISMATCH | diff=4.85 |
| LTV_RISK_CAT | Frequency[High] | 1239 | 1259 | MISMATCH | diff=20 |
| LTV_RISK_CAT | Percent[High] (base excludes missing) | 23.22 | 21.53 | MISMATCH | diff=1.69 |
| LTV_RISK_CAT | table excludes missing level (PROC FREQ default) | excluded | NULL row present | MISMATCH |  |
| LTV_RISK_CAT | Frequency Missing | 0 | - | MISMATCH | not printed |
| DTI_RISK_CAT | Frequency[Low] | 1122 | 1334 | MISMATCH | diff=212 |
| DTI_RISK_CAT | Percent[Low] (base excludes missing) | 26.38 | 22.81 | MISMATCH | diff=3.57 |
| DTI_RISK_CAT | Frequency[Medium] | 2293 | 2440 | MISMATCH | diff=147 |
| DTI_RISK_CAT | Percent[Medium] (base excludes missing) | 53.91 | 41.72 | MISMATCH | diff=12.19 |
| DTI_RISK_CAT | Frequency[High] | 796 | 844 | MISMATCH | diff=48 |
| DTI_RISK_CAT | Percent[High] (base excludes missing) | 18.72 | 14.43 | MISMATCH | diff=4.29 |
| DTI_RISK_CAT | Frequency[Very High] | 42 | 44 | MISMATCH | diff=2 |
| DTI_RISK_CAT | Percent[Very High] (base excludes missing) | 0.99 | 0.75 | MISMATCH | diff=0.24 |
| DTI_RISK_CAT | table excludes missing level (PROC FREQ default) | excluded | NULL row present | MISMATCH |  |
| DTI_RISK_CAT | Frequency Missing | 1084 | - | MISMATCH | not printed |
| DELINQ_RISK_CAT | Frequency[None] | 3872 | 4151 | MISMATCH | diff=279 |
| DELINQ_RISK_CAT | Percent[None] (base excludes missing) | 78.27 | 70.98 | MISMATCH | diff=7.29 |
| DELINQ_RISK_CAT | Frequency[Low] | 598 | 637 | MISMATCH | diff=39 |
| DELINQ_RISK_CAT | Percent[Low] (base excludes missing) | 12.09 | 10.89 | MISMATCH | diff=1.2 |
| DELINQ_RISK_CAT | Frequency[Medium] | 332 | 356 | MISMATCH | diff=24 |
| DELINQ_RISK_CAT | Percent[Medium] (base excludes missing) | 6.71 | 6.09 | MISMATCH | diff=0.62 |
| DELINQ_RISK_CAT | Frequency[High] | 145 | 148 | MISMATCH | diff=3 |
| DELINQ_RISK_CAT | Percent[High] (base excludes missing) | 2.93 | 2.53 | MISMATCH | diff=0.4 |
| DELINQ_RISK_CAT | table excludes missing level (PROC FREQ default) | excluded | NULL row present | MISMATCH |  |
| DELINQ_RISK_CAT | Frequency Missing | 390 | - | MISMATCH | not printed |
| ROW BASE | sum of RISK_SEGMENT frequencies == SAS home_equity_final rows | 5337 | 5848 | MISMATCH | diff=511 |
| Average Default Rate by Risk Segment | Low Risk: N Obs | 3118 | 3603 | MISMATCH | diff=485 |
| Average Default Rate by Risk Segment | Low Risk: N(BAD) (non-missing) | 3118 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Low Risk: Mean(BAD) | 0.152341 | 0.154 | MISMATCH | diff=0.00165876 |
| Average Default Rate by Risk Segment | Low Risk: Std(BAD) (sample, n-1) | 0.359409 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Low Risk: N(LOAN) (non-missing) | 3118 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Low Risk: Mean(LOAN) | 20244.3 | 20012.6 | MISMATCH | diff=231.721 |
| Average Default Rate by Risk Segment | Low Risk: Std(LOAN) (sample, n-1) | 12018.1 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Low Risk: N(LTV) (non-missing) | 3118 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Low Risk: Mean(LTV) | 0.612748 | 0.6127 | MATCH | diff=4.75257e-05 |
| Average Default Rate by Risk Segment | Low Risk: Std(LTV) (sample, n-1) | 0.166188 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Low Risk: N(DEBTINC) (non-missing) | 2409 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Low Risk: Mean(DEBTINC) | 32.0719 | 31.52 | MISMATCH | diff=0.551937 |
| Average Default Rate by Risk Segment | Low Risk: Std(DEBTINC) (sample, n-1) | 6.51328 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Medium Risk: N Obs | 1776 | 1784 | MISMATCH | diff=8 |
| Average Default Rate by Risk Segment | Medium Risk: N(BAD) (non-missing) | 1776 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Medium Risk: Mean(BAD) | 0.216779 | 0.2192 | MISMATCH | diff=0.00242072 |
| Average Default Rate by Risk Segment | Medium Risk: Std(BAD) (sample, n-1) | 0.412167 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Medium Risk: N(LOAN) (non-missing) | 1776 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Medium Risk: Mean(LOAN) | 16580.5 | 16596.8 | MISMATCH | diff=16.2883 |
| Average Default Rate by Risk Segment | Medium Risk: Std(LOAN) (sample, n-1) | 9335.18 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Medium Risk: N(LTV) (non-missing) | 1776 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Medium Risk: Mean(LTV) | 0.774845 | 0.7848 | MISMATCH | diff=0.00995455 |
| Average Default Rate by Risk Segment | Medium Risk: Std(LTV) (sample, n-1) | 0.11686 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Medium Risk: N(DEBTINC) (non-missing) | 1448 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Medium Risk: Mean(DEBTINC) | 36.0048 | 36.02 | MISMATCH | diff=0.0151807 |
| Average Default Rate by Risk Segment | Medium Risk: Std(DEBTINC) (sample, n-1) | 8.56746 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | High Risk: N Obs | 427 | 445 | MISMATCH | diff=18 |
| Average Default Rate by Risk Segment | High Risk: N(BAD) (non-missing) | 427 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | High Risk: Mean(BAD) | 0.283372 | 0.2742 | MISMATCH | diff=0.00917237 |
| Average Default Rate by Risk Segment | High Risk: Std(BAD) (sample, n-1) | 0.451164 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | High Risk: N(LOAN) (non-missing) | 427 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | High Risk: Mean(LOAN) | 14481.3 | 15201.4 | MISMATCH | diff=720.085 |
| Average Default Rate by Risk Segment | High Risk: Std(LOAN) (sample, n-1) | 7228.4 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | High Risk: N(LTV) (non-missing) | 427 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | High Risk: Mean(LTV) | 0.876325 | 1.0638 | MISMATCH | diff=0.187475 |
| Average Default Rate by Risk Segment | High Risk: Std(LTV) (sample, n-1) | 0.398217 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | High Risk: N(DEBTINC) (non-missing) | 381 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | High Risk: Mean(DEBTINC) | 40.5917 | 40.66 | MISMATCH | diff=0.0683419 |
| Average Default Rate by Risk Segment | High Risk: Std(DEBTINC) (sample, n-1) | 7.74862 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Very High Risk: N Obs | 16 | 16 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: N(BAD) (non-missing) | 16 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Very High Risk: Mean(BAD) | 1 | 1 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: Std(BAD) (sample, n-1) | 0 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Very High Risk: N(LOAN) (non-missing) | 16 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Very High Risk: Mean(LOAN) | 17693.8 | 17693.8 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: Std(LOAN) (sample, n-1) | 6972.85 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Very High Risk: N(LTV) (non-missing) | 16 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Very High Risk: Mean(LTV) | 1.01699 | 1.017 | MATCH | diff=9.20596e-06 |
| Average Default Rate by Risk Segment | Very High Risk: Std(LTV) (sample, n-1) | 0.339205 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Very High Risk: N(DEBTINC) (non-missing) | 15 | - | MISMATCH | not printed |
| Average Default Rate by Risk Segment | Very High Risk: Mean(DEBTINC) | 65.7773 | 65.78 | MATCH | diff=0.00273168 |
| Average Default Rate by Risk Segment | Very High Risk: Std(DEBTINC) (sample, n-1) | 33.2453 | - | MISMATCH | not printed |
| Default Rates by LTV Risk and DTI Risk | Low x Low x BAD=0 frequency | 325 | 325 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Low x Low x BAD=1 frequency | 14 | 14 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Low x Medium x BAD=0 frequency | 354 | 354 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Low x Medium x BAD=1 frequency | 29 | 29 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Low x High x BAD=0 frequency | 131 | 131 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Low x High x BAD=1 frequency | 21 | 21 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Low x Very High x BAD=0 frequency | 0 | 0 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Low x Very High x BAD=1 frequency | 9 | 9 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Medium x Low x BAD=0 frequency | 507 | 507 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Medium x Low x BAD=1 frequency | 26 | 26 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Medium x Medium x BAD=0 frequency | 1259 | 1259 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Medium x Medium x BAD=1 frequency | 69 | 69 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Medium x High x BAD=0 frequency | 394 | 394 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Medium x High x BAD=1 frequency | 59 | 59 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Medium x Very High x BAD=0 frequency | 0 | 0 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | Medium x Very High x BAD=1 frequency | 20 | 20 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x Low x BAD=0 frequency | 235 | 235 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x Low x BAD=1 frequency | 15 | 15 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x Medium x BAD=0 frequency | 538 | 538 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x Medium x BAD=1 frequency | 44 | 44 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x High x BAD=0 frequency | 160 | 177 | MISMATCH | diff=17 |
| Default Rates by LTV Risk and DTI Risk | High x High x BAD=1 frequency | 31 | 31 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x Very High x BAD=0 frequency | 0 | 0 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x Very High x BAD=1 frequency | 13 | 13 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | table excludes missing levels (PROC FREQ default) | excluded | NULL rows present | MISMATCH |  |
| Default Rates by LTV Risk and DTI Risk | Frequency Missing | 1084 | - | MISMATCH | not printed |
