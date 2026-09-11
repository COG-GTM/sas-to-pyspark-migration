# Parity results: 05_logistic_regression

Log checked: `validation/05_logistic_regression/logs/run_before_fixes.txt`

**15 / 23 checks matched**

## SAS row base (recomputed in pandas)

- work.model_data rows: 3881
- complete cases for all 10 model variables (PROC LOGISTIC 'Observations Used' share): 3532 (0.9101)
- BAD rate in model_data: 0.0858
- sklearn unpenalised baseline (70/30 split, seed 42): AUC 0.7937, AUPR 0.4517

## Checks

| # | Check | Kind | Expected (SAS semantics) | Actual (PySpark log) | Result | Note |
|---|-------|------|--------------------------|----------------------|--------|------|
| 1 | Step 1 modeling dataset rows (work.model_data) | exact | 3881 | 3548 | MISMATCH | 02 filters (LOAN/VALUE/BAD non-missing, 0<LTV<5, LOAN>0, VALUE>0) + 05 complete cases on LOAN MORTDUE VALUE DEBTINC DELINQ CLAGE |
| 2 | Step 2 train + valid == model rows | exact | 3881 | 3548 | MISMATCH |  |
| 3 | Step 2 training share (SURVEYSELECT samprate=0.7) | tolerance | 0.7000 +/- 0.025 | 0.6545 | MISMATCH | SAS srs would draw exactly round(0.7*3881)=2717; randomSplit is Bernoulli |
| 4 | Step 2 validation share | tolerance | 0.3000 +/- 0.025 | 0.2597 | MISMATCH |  |
| 5 | Step 3 number of assembled features | exact | 16 | 16 | MATCH | 8 numeric + 6 JOB + 2 REASON dummies (SAS param=ref has 14) |
| 6 | Step 3 complete-case share of train used by fit (PROC LOGISTIC 'Number of Observations Used') | tolerance | 0.9101 +/- 0.03 | MISSING | MISMATCH | SAS drops rows with missing DEROG/NINQ/JOB/REASON at fit time; overall share 0.9101 |
| 7 | Step 3 non-zero numeric coefficient signs agree with unpenalised sklearn fit | plausibility | same signs | DEBTINC: spark +0.0667 / sklearn +0.7537; DELINQ: spark +0.5419 / sklearn +0.5726; DEROG: spark +0.6326 / sklearn +0.3894; CLAGE: spark -0.0052 / sklearn -0.3367; NINQ: spark +0.0855 / sklearn +0.1333 | MATCH | L1 (elasticNet) fit vs stepwise MLE: magnitudes not comparable |
| 8 | Step 6 confusion matrix total == validation rows (PROC FREQ keeps every valid row) | exact | 1008 | 1008 | MATCH | SAS: missing pred_prob -> PREDICTED_BAD=0, so no row is dropped from the table |
| 9 | Step 6 validation BAD=1 share vs modeling base rate | tolerance | 0.0858 +/- 0.03 | 0.0962 | MATCH | model_data BAD rate 0.0858 |
| 10 | Step 7 accuracy recomputed from confusion matrix | tolerance | 0.9157 +/- 0.0005 | 0.9157 | MATCH |  |
| 11 | Step 7 weightedPrecision recomputed from confusion matrix | tolerance | 0.9124 +/- 0.0005 | 0.9124 | MATCH |  |
| 12 | Step 7 weightedRecall recomputed from confusion matrix | tolerance | 0.9157 +/- 0.0005 | 0.9157 | MATCH |  |
| 13 | Step 7 f1 (weighted) recomputed from confusion matrix | tolerance | 0.8872 +/- 0.0005 | 0.8872 | MATCH |  |
| 14 | Step 7 AUC vs sklearn complete-case baseline (SAS c-statistic proxy) | tolerance | 0.7937 +/- 0.06 | 0.7642 | MATCH | different split + regularisation; plausibility only |
| 15 | Step 7 AUPR vs sklearn baseline | tolerance | 0.4517 +/- 0.1 | 0.4642 | MATCH | plausibility only |
| 16 | Step 8 N statistic present in by-outcome summary table (agg dict key collision) | presence | printed | MISSING | MISMATCH | PROC MEANS n; {'pred_prob': 'count', 'pred_prob': 'mean'} keeps only the last key |
| 17 | Step 8 N(BAD=0)+N(BAD=1)+unscored == validation rows (PROC MEANS drops missing pred_prob) | exact | 1008 | 1008 | MATCH | unscored (missing predictors) rows printed: None |
| 18 | Step 8 mean pred_prob: BAD=1 > BAD=0 | plausibility | mean1 > mean0 | 0.0687 < 0.2400 | MATCH | sklearn baseline means: 0.0674 / 0.2645 |
| 19 | Step 8 pooled mean pred_prob ~ validation BAD rate (calibration) | tolerance | 0.0962 +/- 0.03 | 0.0852 | MATCH |  |
| 20 | Step 8 BAD=0: min <= p25 <= median <= p75 <= max within [0,1] | presence | all printed & ordered | min=0.00199228, p25=MISSING, median=MISSING, p75=MISSING, max=0.600331 | MISMATCH | SAS PROC MEANS reports p25 median p75 (PCTLDEF=5); Spark percentile() interpolates linearly |
| 21 | Step 8 BAD=0: std printed and in (0, 0.5) | presence | printed | 0.0594841 | MATCH |  |
| 22 | Step 8 BAD=1: min <= p25 <= median <= p75 <= max within [0,1] | presence | all printed & ordered | min=0.000404345, p25=MISSING, median=MISSING, p75=MISSING, max=0.995671 | MISMATCH | SAS PROC MEANS reports p25 median p75 (PCTLDEF=5); Spark percentile() interpolates linearly |
| 23 | Step 8 BAD=1: std printed and in (0, 0.5) | presence | printed | 0.248177 | MATCH |  |
