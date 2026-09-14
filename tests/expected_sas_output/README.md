# Expected SAS output fixtures

Expected output of `sas/03_aggregation_reporting.sas` when run against
`data/home_equity.csv`, one CSV per PROC step, used by
`tests/test_parity_03_aggregation_reporting.py` to diff the PySpark conversion
(`parity/sas_03_aggregation_reporting.py`).

| Fixture | SAS step |
|---|---|
| `03_home_equity_final_nobs.txt` | observation count of `work.home_equity_final` (DATA step chain from `02_data_cleaning.sas`) |
| `03_step1_freq_tables.csv` | `PROC FREQ ... tables JOB REASON LOAN_OUTCOME REGION / nocum` |
| `03_step2_means_by_outcome.csv` | `PROC MEANS n mean median std min max; class LOAN_OUTCOME` |
| `03_step3_tabulate_default_rates.csv` | `PROC TABULATE` JOB x REGION default-rate table, long form incl. `Total` margins |
| `03_step4_sql_top_states.csv` | `PROC SQL outobs=10 ... group by STATE having count(*) >= 10 order by avg_loan desc` |
| `03_step5_means_by_reason_outcome.csv` | `PROC MEANS n mean std median; class REASON LOAN_OUTCOME` |

## Provenance

This repository has no SAS runtime, so the fixtures were **not** exported from a
SAS session. They are produced by `generate_reference_03.py`, a plain-Python
(no Spark, no pandas) row-by-row evaluation of the SAS program that applies
SAS's rules literally: missing sorts below every number, PROC FREQ / CLASS /
TABULATE drop missing levels, `N` counts non-missing analysis values, `STD` is
the sample standard deviation, `MEDIAN` uses `PCTLDEF=5`, `outobs` limits result
rows.

To replace them with real SAS output, run the program with `ODS CSV` (or
`PROC EXPORT` of the `OUT=` datasets) and save the files here using the same
column layouts. Missing values must be written as empty cells.
