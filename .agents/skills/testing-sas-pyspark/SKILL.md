---
name: testing-sas-pyspark
description: Test SAS-to-PySpark migration stages locally for transformation and reporting parity.
---

# Testing SAS-to-PySpark migrations

## Devin Secrets Needed

None.

## Runtime

- OpenJDK 11
- PySpark 3.5.1
- pytest 8.2.2

Verify that dependencies are installed for the active interpreter:

```bash
java -version
python3 -m pip show pyspark pytest
```

If they are missing, run the repository blueprint's maintenance command:

```bash
python3 -m pip install --user "pyspark==3.5.1" "pytest==8.2.2"
```

## Test a migration stage

Run each existing stage from the repository root so relative data paths resolve:

```bash
python3 pyspark/01_data_loading.py
python3 pyspark/02_data_cleaning.py
```

Do not assume every script listed in the README exists on every branch. List
`pyspark/*.py` first and run only the stages present on the target branch.

For stage 02, the current dataset should produce:

```text
Original rows: 5960
After filtering: 5848
Final clean rows: 5337
```

Validate both real-data output and synthetic boundary rows. Synthetic coverage
should include:

- proper-casing and loan-outcome derivation;
- missing-value flags;
- an LTV just below `5`, which remains;
- an LTV equal to `5`, which is removed;
- zero or missing property value, which produces null LTV or is filtered.

## Validation commands

```bash
python3 -m compileall -q pyspark tests
python3 -m pytest tests/test_pyspark_outputs.py -v
```

If the frequency-count test fails with `5960 != 5681`, check whether the
target branch still counts the null `JOB` group before treating it as a
migration regression.
