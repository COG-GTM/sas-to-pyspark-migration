# SAS to PySpark Migration Demo

This repository demonstrates how [Devin](https://devin.ai) can accelerate SAS-to-PySpark migration for financial services organizations. Using a real-world home equity loan dataset, it provides worked migration examples (data loading, data cleaning) — with side-by-side SAS and PySpark code and inline comments mapping each SAS construct to its PySpark equivalent — alongside the original SAS programs that are ready to be migrated, plus a validation suite to check correctness.

Built for teams evaluating a move from SAS to open-source distributed computing on platforms like Databricks, Amazon EMR, or Azure Synapse.

---

## Why Migrate?

| Driver | Details |
|---|---|
| **Scalability** | PySpark runs on distributed clusters, processing terabytes of data across hundreds of nodes — far beyond SAS's single-server architecture. |
| **Cost** | PySpark is open-source (Apache 2.0). Eliminating SAS licensing fees can save millions annually for large enterprises. |
| **Cloud-Native** | PySpark is the native compute engine for Databricks, Amazon EMR, Azure Synapse, and Google Dataproc — enabling seamless cloud adoption. |
| **Modern ML Ecosystem** | Direct integration with scikit-learn, TensorFlow, PyTorch, MLflow, and the broader Python data science ecosystem. |

---

## Migration Examples

### Completed

| # | SAS Program | PySpark Script | Description |
|---|---|---|---|
| 1 | `sas/01_data_loading.sas` | `pyspark/01_data_loading.py` | Load CSV, apply labels/formats, inspect metadata, preview data |
| 2 | `sas/02_data_cleaning.sas` | `pyspark/02_data_cleaning.py` | Derived columns (LTV, LOAN_OUTCOME), missing-value flags, filtering, outlier stats |

### To be migrated

The remaining SAS programs are included as migration targets and have not yet been ported to PySpark:

- `sas/03_aggregation_reporting.sas`
- `sas/04_risk_segmentation.sas`
- `sas/05_logistic_regression.sas`

---

## How Devin Helps

Devin automates the heavy lifting of SAS-to-PySpark migration:

1. **Reads and understands SAS code** — Devin parses DATA steps, PROC procedures, macro variables, and SAS-specific constructs to understand the full intent of each program.

2. **Generates idiomatic PySpark equivalents** — Rather than a literal line-by-line translation, Devin produces clean, Pythonic PySpark code that follows Spark best practices (lazy evaluation, DataFrame API, ML Pipelines).

3. **Writes validation tests** — Devin automatically generates test suites that verify row counts, column existence, derived column calculations, and model outputs match expected results.

4. **Documents the migration mapping** — Devin produces comprehensive reference guides mapping every SAS construct (PROC MEANS, PROC FREQ, DATA step arrays, macro variables) to its PySpark equivalent.

---

## Migration Patterns

The [`migration_guide/`](migration_guide/) folder contains detailed reference documentation:

- **[SAS to PySpark Mapping](migration_guide/sas_to_pyspark_mapping.md)** — Complete reference table covering DATA step operations, statistical procedures, ML, macro language, and data management constructs.
- **[Common Patterns](migration_guide/common_patterns.md)** — Seven detailed side-by-side examples of the most frequently encountered migration patterns, including data loading, conditional logic, missing values, aggregation, joins, macros, and output writing.

---

## Getting Started

### Prerequisites

- Python 3.8+
- Java 8 or 11 (required by PySpark)

### Installation

```bash
pip install pyspark
```

### Running the PySpark Scripts

Each script is self-contained and can be run independently:

```bash
# From the repository root directory
python pyspark/01_data_loading.py
```

### Running the Tests

```bash
python -m pytest tests/test_pyspark_outputs.py -v
```

Or with unittest directly:

```bash
python -m unittest tests.test_pyspark_outputs -v
```

---

## Dataset

The `data/home_equity.csv` file contains 5,960 home equity loan records with 18 columns:

| Column | Description |
|---|---|
| `BAD` | Loan default flag (1 = default, 0 = paid) |
| `LOAN` | Amount of loan request |
| `MORTDUE` | Amount due on existing mortgage |
| `VALUE` | Value of current property |
| `REASON` | Loan purpose (HomeImp or DebtCon) |
| `JOB` | Job category |
| `YOJ` | Years at present job |
| `DEROG` | Number of derogatory reports |
| `DELINQ` | Number of delinquent credit lines |
| `CLAGE` | Age of oldest credit line (months) |
| `NINQ` | Number of recent credit inquiries |
| `CLNO` | Number of credit lines |
| `DEBTINC` | Debt to income ratio |
| `APPDATE` | Loan application date |
| `CITY` | City |
| `STATE` | State |
| `DIVISION` | Census division |
| `REGION` | Census region |

Source: [COG-GTM/sas-viya-quick-start](https://github.com/COG-GTM/sas-viya-quick-start)

---

## Repository Structure

```
sas-to-pyspark-migration/
├── README.md                                  # This file
├── data/
│   └── home_equity.csv                        # Home equity loan dataset (5,960 records)
├── sas/
│   ├── 01_data_loading.sas                    # PROC IMPORT, PROC CONTENTS, PROC PRINT
│   ├── 02_data_cleaning.sas                   # DATA step, arrays, PROC MEANS
│   ├── 03_aggregation_reporting.sas           # PROC FREQ, PROC MEANS, PROC TABULATE, PROC SQL
│   ├── 04_risk_segmentation.sas               # PROC FORMAT, risk scoring, PROC FREQ
│   └── 05_logistic_regression.sas             # PROC LOGISTIC, stepwise, ROC/AUC
├── pyspark/
│   ├── 01_data_loading.py                     # spark.read.csv, printSchema, show
│   └── 02_data_cleaning.py                    # when/otherwise, missing flags, filter, summary
├── migration_guide/
│   ├── sas_to_pyspark_mapping.md              # Complete SAS → PySpark construct reference
│   └── common_patterns.md                     # Side-by-side migration pattern examples
└── tests/
    └── test_pyspark_outputs.py                # Validation tests for all PySpark scripts
```

---

## License

This project is provided as a demonstration. The home equity dataset is sourced from [SAS Viya Quick Start](https://github.com/COG-GTM/sas-viya-quick-start) materials.
