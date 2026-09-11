# Parity results: 01_data_loading

SAS reference: `sas/01_data_loading.sas`  
PySpark stage: `pyspark/01_data_loading.py`  
Stage log parsed: `validation/01_data_loading/logs/run_before_fixes.txt`  
Expected values: recomputed with pandas / csv from `data/home_equity.csv` and the LABEL statement of the SAS source (no Spark).

Row base: PROC IMPORT loads every CSV record (5960 rows, 18 variables). Stage 01 applies no filters; the `0 < LTV < 5`, `LOAN > 0`, `VALUE > 0` and non-missing LOAN/VALUE/BAD filters belong to `02_data_cleaning.sas` and are not expected here.

Tolerances: numeric cells compared with rel_tol=1e-09 / abs_tol=1e-09; SAS missing (`.` / blank) matched against Spark `NULL`; strings compared exactly.

## Summary

**61 / 61 checks matched**

## Checks

| # | Section | Check | Expected (SAS semantics) | Actual (PySpark log) | Match | Note |
|---|---------|-------|--------------------------|----------------------|-------|------|
| 1 | PROC CONTENTS | Number of observations | 5960 | 5960 | OK |  |
| 2 | PROC CONTENTS | Number of variables | 18 | 18 | OK |  |
| 3 | PROC CONTENTS | Variable names (file order) | ['BAD', 'LOAN', 'MORTDUE', 'VALUE', 'REASON', 'JOB', 'YOJ', 'DEROG', 'DELINQ', 'CLAGE', 'NINQ', 'CLNO', 'DEBTINC', 'A... | ['BAD', 'LOAN', 'MORTDUE', 'VALUE', 'REASON', 'JOB', 'YOJ', 'DEROG', 'DELINQ', 'CLAGE', 'NINQ', 'CLNO', 'DEBTINC', 'A... | OK |  |
| 4 | PROC CONTENTS type | BAD | Num | Num (integer) | OK |  |
| 5 | PROC CONTENTS type | LOAN | Num | Num (integer) | OK |  |
| 6 | PROC CONTENTS type | MORTDUE | Num | Num (double) | OK |  |
| 7 | PROC CONTENTS type | VALUE | Num | Num (double) | OK |  |
| 8 | PROC CONTENTS type | REASON | Char | Char (string) | OK |  |
| 9 | PROC CONTENTS type | JOB | Char | Char (string) | OK |  |
| 10 | PROC CONTENTS type | YOJ | Num | Num (double) | OK |  |
| 11 | PROC CONTENTS type | DEROG | Num | Num (integer) | OK |  |
| 12 | PROC CONTENTS type | DELINQ | Num | Num (integer) | OK |  |
| 13 | PROC CONTENTS type | CLAGE | Num | Num (double) | OK |  |
| 14 | PROC CONTENTS type | NINQ | Num | Num (integer) | OK |  |
| 15 | PROC CONTENTS type | CLNO | Num | Num (integer) | OK |  |
| 16 | PROC CONTENTS type | DEBTINC | Num | Num (double) | OK |  |
| 17 | PROC CONTENTS type | APPDATE | Num | Num (integer) | OK |  |
| 18 | PROC CONTENTS type | CITY | Char | Char (string) | OK |  |
| 19 | PROC CONTENTS type | STATE | Char | Char (string) | OK |  |
| 20 | PROC CONTENTS type | DIVISION | Char | Char (string) | OK |  |
| 21 | PROC CONTENTS type | REGION | Char | Char (string) | OK |  |
| 22 | LABEL | BAD | Loan Status (1=Default, 0=Paid) | Loan Status (1=Default, 0=Paid) | OK |  |
| 23 | LABEL | LOAN | Amount of Loan Request | Amount of Loan Request | OK |  |
| 24 | LABEL | MORTDUE | Amount Due on Existing Mortgage | Amount Due on Existing Mortgage | OK |  |
| 25 | LABEL | VALUE | Value of Current Property | Value of Current Property | OK |  |
| 26 | LABEL | REASON | Loan Purpose (HomeImp or DebtCon) | Loan Purpose (HomeImp or DebtCon) | OK |  |
| 27 | LABEL | JOB | Job Category | Job Category | OK |  |
| 28 | LABEL | YOJ | Years at Present Job | Years at Present Job | OK |  |
| 29 | LABEL | DEROG | Number of Derogatory Reports | Number of Derogatory Reports | OK |  |
| 30 | LABEL | DELINQ | Number of Delinquent Credit Lines | Number of Delinquent Credit Lines | OK |  |
| 31 | LABEL | CLAGE | Age of Oldest Credit Line (months) | Age of Oldest Credit Line (months) | OK |  |
| 32 | LABEL | NINQ | Number of Recent Credit Inquiries | Number of Recent Credit Inquiries | OK |  |
| 33 | LABEL | CLNO | Number of Credit Lines | Number of Credit Lines | OK |  |
| 34 | LABEL | DEBTINC | Debt to Income Ratio | Debt to Income Ratio | OK |  |
| 35 | LABEL | APPDATE | Loan Application Date | Loan Application Date | OK |  |
| 36 | LABEL | CITY | City | City | OK |  |
| 37 | LABEL | STATE | State | State | OK |  |
| 38 | LABEL | DIVISION | Census Division | Census Division | OK |  |
| 39 | LABEL | REGION | Census Region | Census Region | OK |  |
| 40 | PROC PRINT | Preview header | ['BAD', 'LOAN', 'MORTDUE', 'VALUE', 'REASON', 'JOB', 'YOJ', 'DEROG', 'DELINQ', 'CLAGE', 'NINQ', 'CLNO', 'DEBTINC', 'A... | ['BAD', 'LOAN', 'MORTDUE', 'VALUE', 'REASON', 'JOB', 'YOJ', 'DEROG', 'DELINQ', 'CLAGE', 'NINQ', 'CLNO', 'DEBTINC', 'A... | OK |  |
| 41 | PROC PRINT | Preview row count | 20 | 20 | OK |  |
| 42 | PROC PRINT | Obs 1 (18 cells) | BAD=1 LOAN=1100 CITY=oregon | BAD=1 LOAN=1100 CITY=oregon | OK |  |
| 43 | PROC PRINT | Obs 2 (18 cells) | BAD=1 LOAN=1300 CITY=churchton | BAD=1 LOAN=1300 CITY=churchton | OK |  |
| 44 | PROC PRINT | Obs 3 (18 cells) | BAD=1 LOAN=1500 CITY=orcas | BAD=1 LOAN=1500 CITY=orcas | OK |  |
| 45 | PROC PRINT | Obs 4 (18 cells) | BAD=1 LOAN=1500 CITY=hastings | BAD=1 LOAN=1500 CITY=hastings | OK |  |
| 46 | PROC PRINT | Obs 5 (18 cells) | BAD=0 LOAN=1700 CITY=wilmington | BAD=0 LOAN=1700 CITY=wilmington | OK |  |
| 47 | PROC PRINT | Obs 6 (18 cells) | BAD=1 LOAN=1700 CITY=olympia | BAD=1 LOAN=1700 CITY=olympia | OK |  |
| 48 | PROC PRINT | Obs 7 (18 cells) | BAD=1 LOAN=1800 CITY=williston | BAD=1 LOAN=1800 CITY=williston | OK |  |
| 49 | PROC PRINT | Obs 8 (18 cells) | BAD=1 LOAN=1800 CITY=longview | BAD=1 LOAN=1800 CITY=longview | OK |  |
| 50 | PROC PRINT | Obs 9 (18 cells) | BAD=1 LOAN=2000 CITY=elma | BAD=1 LOAN=2000 CITY=elma | OK |  |
| 51 | PROC PRINT | Obs 10 (18 cells) | BAD=1 LOAN=2000 CITY=montclair | BAD=1 LOAN=2000 CITY=montclair | OK |  |
| 52 | PROC PRINT | Obs 11 (18 cells) | BAD=1 LOAN=2000 CITY=whittier | BAD=1 LOAN=2000 CITY=whittier | OK |  |
| 53 | PROC PRINT | Obs 12 (18 cells) | BAD=1 LOAN=2000 CITY=orlando | BAD=1 LOAN=2000 CITY=orlando | OK |  |
| 54 | PROC PRINT | Obs 13 (18 cells) | BAD=1 LOAN=2000 CITY=elm grove | BAD=1 LOAN=2000 CITY=elm grove | OK |  |
| 55 | PROC PRINT | Obs 14 (18 cells) | BAD=0 LOAN=2000 CITY=hatboro | BAD=0 LOAN=2000 CITY=hatboro | OK |  |
| 56 | PROC PRINT | Obs 15 (18 cells) | BAD=1 LOAN=2100 CITY=linden | BAD=1 LOAN=2100 CITY=linden | OK |  |
| 57 | PROC PRINT | Obs 16 (18 cells) | BAD=1 LOAN=2200 CITY=wildomar | BAD=1 LOAN=2200 CITY=wildomar | OK |  |
| 58 | PROC PRINT | Obs 17 (18 cells) | BAD=1 LOAN=2200 CITY=minier | BAD=1 LOAN=2200 CITY=minier | OK |  |
| 59 | PROC PRINT | Obs 18 (18 cells) | BAD=1 LOAN=2200 CITY=los banos | BAD=1 LOAN=2200 CITY=los banos | OK |  |
| 60 | PROC PRINT | Obs 19 (18 cells) | BAD=1 LOAN=2300 CITY=anchorage | BAD=1 LOAN=2300 CITY=anchorage | OK |  |
| 61 | PROC PRINT | Obs 20 (18 cells) | BAD=0 LOAN=2300 CITY=lufkin | BAD=0 LOAN=2300 CITY=lufkin | OK |  |

## Not applicable to this stage

- No PROC FREQ / PROC MEANS / medians / model AUC in `01_data_loading.sas`; nothing to compare.
- SAS display formats (dollar12., date9., 8.1, comma8.1) affect rendering only, not values; PySpark prints raw values (e.g. `25860.0` for `$25,860`) - harmless.
