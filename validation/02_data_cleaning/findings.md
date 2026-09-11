# 02_data_cleaning parity findings

Row base (from `sas/02_data_cleaning.sas`): 5960 raw rows -> `home_equity_filtered`
(LOAN, VALUE, BAD non-missing) = 5848 rows -> `home_equity_final`
(0 < LTV < 5, LOAN > 0, VALUE > 0) = 5337 rows. Downstream stages inherit these filters.

Before fixes: 92 / 147 checks matched (`parity_results_before_fixes.md`, log `logs/run_before_fixes.txt`).
After fixes: 147 / 147 checks matched (`parity_results.md`, log `logs/run.txt`).

## Real migration bugs (fixed in `pyspark/02_data_cleaning.py`)

1. Percentiles wildly off (e.g. LTV p99 = 7.1625 vs SAS 0.9965, LOAN p1 = 1100 vs 3600)
   -> `approxQuantile(..., relativeError=0.01)` is +/-58 ranks on 5848 rows, collapsing p1/p99 to min/max
   -> use `relativeError=0.0` (exact).
2. PROC MEANS percentiles printed for only 5 of the 8 `var` variables (CLAGE, DEROG, DELINQ missing)
   -> hard-coded shorter column list -> loop over the full PROC MEANS variable list.
3. `nmiss` statistic requested by both PROC MEANS calls was not printed
   -> `describe()` has no NMISS -> `describeWithNmiss()` appends a per-variable missing-count row.

## Harmless differences

- Spark exact `approxQuantile` returns the observation at rank ceil(n*p); SAS PCTLDEF=5 averages the
  two neighbours when n*p is an integer (e.g. VALUE p25 66056 vs 66069, median 89231 vs 89235.5).
  Compared with a 1% relative tolerance.
- Float formatting (`2063.0` vs SAS `$2,063`, `percent8.2` on LTV), `NULL` vs SAS `.`.
- Spark `describe()` labels (`count`/`stddev`) vs SAS `N`/`Std Dev`.
- `initcap(CITY)` vs `propcase(CITY)`: CITY is never printed by this stage, so not verifiable here;
  both title-case whitespace-delimited words, but SAS propcase also splits on `/ - ( . '`.
- "Original rows / After filtering / Final clean rows" have no SAS printout; they are checked against
  the SAS row base above.
