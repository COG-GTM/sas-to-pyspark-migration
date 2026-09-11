# Parity results: 04_risk_segmentation

Log checked: `validation/logs/04_risk_segmentation.txt`

SAS row base (work.home_equity_final): **5337** rows (LOAN/VALUE/BAD non-missing, 0 < LTV < 5, LOAN > 0, VALUE > 0).

## Summary: 117 / 117 checks matched

| Section | Check | Expected (SAS semantics) | Actual (PySpark log) | Status | Detail |
|---|---|---|---|---|---|
| RISK_SEGMENT | Frequency[Low Risk] | 3118 | 3118 | MATCH | diff=0 |
| RISK_SEGMENT | Percent[Low Risk] (base excludes missing) | 58.42 | 58.42 | MATCH | diff=0 |
| RISK_SEGMENT | Frequency[Medium Risk] | 1776 | 1776 | MATCH | diff=0 |
| RISK_SEGMENT | Percent[Medium Risk] (base excludes missing) | 33.28 | 33.28 | MATCH | diff=0 |
| RISK_SEGMENT | Frequency[High Risk] | 427 | 427 | MATCH | diff=0 |
| RISK_SEGMENT | Percent[High Risk] (base excludes missing) | 8 | 8 | MATCH | diff=0 |
| RISK_SEGMENT | Frequency[Very High Risk] | 16 | 16 | MATCH | diff=0 |
| RISK_SEGMENT | Percent[Very High Risk] (base excludes missing) | 0.3 | 0.3 | MATCH | diff=0 |
| RISK_SEGMENT | table excludes missing level (PROC FREQ default) | excluded | excluded | MATCH |  |
| RISK_SEGMENT | Frequency Missing | 0 | 0 | MATCH | diff=0 |
| LTV_RISK_CAT | Frequency[Low] | 1131 | 1131 | MATCH | diff=0 |
| LTV_RISK_CAT | Percent[Low] (base excludes missing) | 21.19 | 21.19 | MATCH | diff=0 |
| LTV_RISK_CAT | Frequency[Medium] | 2967 | 2967 | MATCH | diff=0 |
| LTV_RISK_CAT | Percent[Medium] (base excludes missing) | 55.59 | 55.59 | MATCH | diff=0 |
| LTV_RISK_CAT | Frequency[High] | 1239 | 1239 | MATCH | diff=0 |
| LTV_RISK_CAT | Percent[High] (base excludes missing) | 23.22 | 23.22 | MATCH | diff=0 |
| LTV_RISK_CAT | table excludes missing level (PROC FREQ default) | excluded | excluded | MATCH |  |
| LTV_RISK_CAT | Frequency Missing | 0 | 0 | MATCH | diff=0 |
| DTI_RISK_CAT | Frequency[Low] | 1122 | 1122 | MATCH | diff=0 |
| DTI_RISK_CAT | Percent[Low] (base excludes missing) | 26.38 | 26.38 | MATCH | diff=0 |
| DTI_RISK_CAT | Frequency[Medium] | 2293 | 2293 | MATCH | diff=0 |
| DTI_RISK_CAT | Percent[Medium] (base excludes missing) | 53.91 | 53.91 | MATCH | diff=0 |
| DTI_RISK_CAT | Frequency[High] | 796 | 796 | MATCH | diff=0 |
| DTI_RISK_CAT | Percent[High] (base excludes missing) | 18.72 | 18.72 | MATCH | diff=0 |
| DTI_RISK_CAT | Frequency[Very High] | 42 | 42 | MATCH | diff=0 |
| DTI_RISK_CAT | Percent[Very High] (base excludes missing) | 0.99 | 0.99 | MATCH | diff=0 |
| DTI_RISK_CAT | table excludes missing level (PROC FREQ default) | excluded | excluded | MATCH |  |
| DTI_RISK_CAT | Frequency Missing | 1084 | 1084 | MATCH | diff=0 |
| DELINQ_RISK_CAT | Frequency[None] | 3872 | 3872 | MATCH | diff=0 |
| DELINQ_RISK_CAT | Percent[None] (base excludes missing) | 78.27 | 78.27 | MATCH | diff=0 |
| DELINQ_RISK_CAT | Frequency[Low] | 598 | 598 | MATCH | diff=0 |
| DELINQ_RISK_CAT | Percent[Low] (base excludes missing) | 12.09 | 12.09 | MATCH | diff=0 |
| DELINQ_RISK_CAT | Frequency[Medium] | 332 | 332 | MATCH | diff=0 |
| DELINQ_RISK_CAT | Percent[Medium] (base excludes missing) | 6.71 | 6.71 | MATCH | diff=0 |
| DELINQ_RISK_CAT | Frequency[High] | 145 | 145 | MATCH | diff=0 |
| DELINQ_RISK_CAT | Percent[High] (base excludes missing) | 2.93 | 2.93 | MATCH | diff=0 |
| DELINQ_RISK_CAT | table excludes missing level (PROC FREQ default) | excluded | excluded | MATCH |  |
| DELINQ_RISK_CAT | Frequency Missing | 390 | 390 | MATCH | diff=0 |
| ROW BASE | sum of RISK_SEGMENT frequencies == SAS home_equity_final rows | 5337 | 5337 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Low Risk: N Obs | 3118 | 3118 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Low Risk: N(BAD) (non-missing) | 3118 | 3118 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Low Risk: Mean(BAD) | 0.152341 | 0.1523 | MATCH | diff=4.12444e-05 |
| Average Default Rate by Risk Segment | Low Risk: Std(BAD) (sample, n-1) | 0.359409 | 0.3594 | MATCH | diff=8.98485e-06 |
| Average Default Rate by Risk Segment | Low Risk: N(LOAN) (non-missing) | 3118 | 3118 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Low Risk: Mean(LOAN) | 20244.3 | 20244.3 | MATCH | diff=1.23156e-05 |
| Average Default Rate by Risk Segment | Low Risk: Std(LOAN) (sample, n-1) | 12018.1 | 12018.1 | MATCH | diff=4.7492e-05 |
| Average Default Rate by Risk Segment | Low Risk: N(LTV) (non-missing) | 3118 | 3118 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Low Risk: Mean(LTV) | 0.612748 | 0.6127 | MATCH | diff=4.75257e-05 |
| Average Default Rate by Risk Segment | Low Risk: Std(LTV) (sample, n-1) | 0.166188 | 0.1662 | MATCH | diff=1.18462e-05 |
| Average Default Rate by Risk Segment | Low Risk: N(DEBTINC) (non-missing) | 2409 | 2409 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Low Risk: Mean(DEBTINC) | 32.0719 | 32.0719 | MATCH | diff=3.71801e-05 |
| Average Default Rate by Risk Segment | Low Risk: Std(DEBTINC) (sample, n-1) | 6.51328 | 6.5133 | MATCH | diff=2.10539e-05 |
| Average Default Rate by Risk Segment | Medium Risk: N Obs | 1776 | 1776 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Medium Risk: N(BAD) (non-missing) | 1776 | 1776 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Medium Risk: Mean(BAD) | 0.216779 | 0.2168 | MATCH | diff=2.07207e-05 |
| Average Default Rate by Risk Segment | Medium Risk: Std(BAD) (sample, n-1) | 0.412167 | 0.4122 | MATCH | diff=3.29496e-05 |
| Average Default Rate by Risk Segment | Medium Risk: N(LOAN) (non-missing) | 1776 | 1776 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Medium Risk: Mean(LOAN) | 16580.5 | 16580.5 | MATCH | diff=1.17117e-05 |
| Average Default Rate by Risk Segment | Medium Risk: Std(LOAN) (sample, n-1) | 9335.18 | 9335.18 | MATCH | diff=4.97983e-05 |
| Average Default Rate by Risk Segment | Medium Risk: N(LTV) (non-missing) | 1776 | 1776 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Medium Risk: Mean(LTV) | 0.774845 | 0.7748 | MATCH | diff=4.54531e-05 |
| Average Default Rate by Risk Segment | Medium Risk: Std(LTV) (sample, n-1) | 0.11686 | 0.1169 | MATCH | diff=3.95843e-05 |
| Average Default Rate by Risk Segment | Medium Risk: N(DEBTINC) (non-missing) | 1448 | 1448 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Medium Risk: Mean(DEBTINC) | 36.0048 | 36.0048 | MATCH | diff=1.93141e-05 |
| Average Default Rate by Risk Segment | Medium Risk: Std(DEBTINC) (sample, n-1) | 8.56746 | 8.5675 | MATCH | diff=3.90659e-05 |
| Average Default Rate by Risk Segment | High Risk: N Obs | 427 | 427 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | High Risk: N(BAD) (non-missing) | 427 | 427 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | High Risk: Mean(BAD) | 0.283372 | 0.2834 | MATCH | diff=2.76347e-05 |
| Average Default Rate by Risk Segment | High Risk: Std(BAD) (sample, n-1) | 0.451164 | 0.4512 | MATCH | diff=3.57684e-05 |
| Average Default Rate by Risk Segment | High Risk: N(LOAN) (non-missing) | 427 | 427 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | High Risk: Mean(LOAN) | 14481.3 | 14481.3 | MATCH | diff=3.70023e-05 |
| Average Default Rate by Risk Segment | High Risk: Std(LOAN) (sample, n-1) | 7228.4 | 7228.4 | MATCH | diff=5.22024e-06 |
| Average Default Rate by Risk Segment | High Risk: N(LTV) (non-missing) | 427 | 427 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | High Risk: Mean(LTV) | 0.876325 | 0.8763 | MATCH | diff=2.47201e-05 |
| Average Default Rate by Risk Segment | High Risk: Std(LTV) (sample, n-1) | 0.398217 | 0.3982 | MATCH | diff=1.7067e-05 |
| Average Default Rate by Risk Segment | High Risk: N(DEBTINC) (non-missing) | 381 | 381 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | High Risk: Mean(DEBTINC) | 40.5917 | 40.5917 | MATCH | diff=4.19162e-05 |
| Average Default Rate by Risk Segment | High Risk: Std(DEBTINC) (sample, n-1) | 7.74862 | 7.7486 | MATCH | diff=1.68147e-05 |
| Average Default Rate by Risk Segment | Very High Risk: N Obs | 16 | 16 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: N(BAD) (non-missing) | 16 | 16 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: Mean(BAD) | 1 | 1 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: Std(BAD) (sample, n-1) | 0 | 0 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: N(LOAN) (non-missing) | 16 | 16 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: Mean(LOAN) | 17693.8 | 17693.8 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: Std(LOAN) (sample, n-1) | 6972.85 | 6972.85 | MATCH | diff=3.07356e-05 |
| Average Default Rate by Risk Segment | Very High Risk: N(LTV) (non-missing) | 16 | 16 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: Mean(LTV) | 1.01699 | 1.017 | MATCH | diff=9.20596e-06 |
| Average Default Rate by Risk Segment | Very High Risk: Std(LTV) (sample, n-1) | 0.339205 | 0.3392 | MATCH | diff=4.75445e-06 |
| Average Default Rate by Risk Segment | Very High Risk: N(DEBTINC) (non-missing) | 15 | 15 | MATCH | diff=0 |
| Average Default Rate by Risk Segment | Very High Risk: Mean(DEBTINC) | 65.7773 | 65.7773 | MATCH | diff=3.16761e-05 |
| Average Default Rate by Risk Segment | Very High Risk: Std(DEBTINC) (sample, n-1) | 33.2453 | 33.2453 | MATCH | diff=1.6977e-05 |
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
| Default Rates by LTV Risk and DTI Risk | High x High x BAD=0 frequency | 160 | 160 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x High x BAD=1 frequency | 31 | 31 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x Very High x BAD=0 frequency | 0 | 0 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | High x Very High x BAD=1 frequency | 13 | 13 | MATCH | diff=0 |
| Default Rates by LTV Risk and DTI Risk | table excludes missing levels (PROC FREQ default) | excluded | excluded | MATCH |  |
| Default Rates by LTV Risk and DTI Risk | Frequency Missing | 1084 | 1084 | MATCH | diff=0 |
