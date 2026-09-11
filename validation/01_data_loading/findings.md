# Findings: 01_data_loading

Pre-fix parity: 61 / 61 checks matched (`parity_results_before_fixes.md`, log `logs/run_before_fixes.txt`).
Post-fix parity: 61 / 61 checks matched (`parity_results.md`, log `logs/run.txt`, exit code 0).

## Row base

`sas/01_data_loading.sas` runs PROC IMPORT on the full CSV and applies no filters: 5960 observations,
18 variables. The `0 < LTV < 5`, `LOAN > 0`, `VALUE > 0` and non-missing LOAN/VALUE/BAD filters are
introduced by `sas/02_data_cleaning.sas` and are not part of this stage's contract.

## Real migration bugs

None found. Every printed number (row count, column count, column names/order, Num/Char type of each
of the 18 variables, the 18 variable labels, and all 360 cells of the 20-observation preview) matches
the pandas recomputation of the SAS semantics.

## Harmless differences

- SAS display formats (`dollar12.`, `date9.`, `8.1`, `comma8.1`) are rendering-only; PySpark prints raw
  values (`25860.0` instead of `$25,860`; APPDATE stays a SAS day count such as `22040`).
- Spark infers `integer` vs `double` per column (e.g. LOAN integer, MORTDUE double) where SAS has a single
  Num type; values are identical.
- Spark prints `NULL` where SAS prints `.` (numeric) or blank (character).
- SAS labels are stored as dataset metadata; PySpark prints them from a dictionary.

## Non-parity edits

- Removed five unused `pyspark.sql.types` imports from `pyspark/01_data_loading.py` (flake8 F401).
  No behavioural change.

## Test-suite observations (not changed)

- `pytest -k data` selects only `test_data_loads_successfully`; the other stage-01 tests
  (`test_row_count`, `test_column_count`, `test_expected_columns_exist`) need
  `-k "data or row_count or column"`. All four pass (`logs/pytest_stage01.txt`).
- The tests re-implement the transformations inline instead of importing the stage scripts, so they do not
  exercise `pyspark/01_data_loading.py` directly.
