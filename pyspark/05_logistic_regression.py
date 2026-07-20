"""
PySpark Script: 05_logistic_regression.py
Purpose: Build a logistic regression model for loan default prediction
  - Logistic regression (equivalent to PROC LOGISTIC with stepwise selection)
  - Model evaluation (concordance, AUC)
  - Predicted probabilities and confusion matrix
Equivalent SAS Program: sas/05_logistic_regression.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, mean, stddev, min as spark_min, \
    max as spark_max, expr
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Initialize SparkSession
# SAS equivalent: Starting a SAS session
spark = SparkSession.builder \
    .appName("HomeEquity_LogisticRegression") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------
# Upstream data preparation (self-contained)
# In SAS this program consumes work.home_equity_risk, which is built by
# 02_data_cleaning.sas (-> work.home_equity_final) and 04_risk_segmentation.sas.
# The logistic model below only uses the cleaned columns, so we re-derive the
# cleaned dataset (work.home_equity_final) directly from data/home_equity.csv,
# reproducing the transformations/filters from 02_data_cleaning.sas.
# The risk-segment columns added in 04 are not model inputs and are omitted.
# ------------------------------------------------------------------
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)

# SAS equivalent (02_data_cleaning.sas):
#   if VALUE ne . and MORTDUE ne . and VALUE > 0 then LTV = MORTDUE / VALUE;
#   if LOAN ne . and VALUE ne . and BAD ne .;   (filtered)
#   if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0;   (home_equity_final)
df = df \
    .withColumn(
        "LTV",
        when(
            (col("VALUE").isNotNull()) & (col("MORTDUE").isNotNull()) & (col("VALUE") > 0),
            col("MORTDUE") / col("VALUE")
        )
    ) \
    .filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull() &
        (col("LTV") > 0) & (col("LTV") < 5) &
        (col("LOAN") > 0) & (col("VALUE") > 0)
    )

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - keep complete cases for key predictors
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .;
#   run;
# ------------------------------------------------------------------
model_data = df.filter(
    col("LOAN").isNotNull() & col("MORTDUE").isNotNull() & col("VALUE").isNotNull() &
    col("DEBTINC").isNotNull() & col("DELINQ").isNotNull() & col("CLAGE").isNotNull()
)

print("=" * 60)
print("Modeling dataset prepared (complete cases)")
print("=" * 60)
print(f"Rows available for modeling: {model_data.count()}")

# ------------------------------------------------------------------
# Step 2: Split into training (70%) and validation (30%)
# SAS equivalent:
#   proc surveyselect data=work.model_data out=work.model_split
#       method=srs samprate=0.7 seed=42;
#   run;
#   data work.train work.valid; set work.model_split;
#       if selected = 1 then output work.train; else output work.valid;
#   run;
#
# PySpark uses randomSplit for simple random sampling (method=srs).
# ------------------------------------------------------------------
train, valid = model_data.randomSplit([0.7, 0.3], seed=42)
print(f"Training rows:   {train.count()}")
print(f"Validation rows: {valid.count()}")

# ------------------------------------------------------------------
# Step 3: Fit logistic regression
# SAS equivalent:
#   title "Logistic Regression: Loan Default Prediction";
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ JOB REASON
#             / selection=stepwise slentry=0.05 slstay=0.05 details lackfit;
#       output out=work.train_scored predicted=pred_prob;
#       store work.logit_model;
#   run;
#
# Notes on SAS-only constructs:
#   - 'descending' models P(BAD=1); Spark LogisticRegression already predicts
#     probability of label 1, so BAD is used directly as the label.
#   - CLASS ... / param=ref reference coding is reproduced with StringIndexer +
#     OneHotEncoder (Spark drops the last indexed level as the reference).
#   - selection=stepwise / slentry / slstay: Spark ML has no built-in stepwise
#     selection, so the full specified model is fit (all effects retained).
#   - 'lackfit' (Hosmer-Lemeshow) has no direct Spark equivalent; omitted.
#   - handleInvalid='skip' reproduces PROC LOGISTIC listwise deletion of rows
#     with any missing model variable (e.g. missing DEROG/NINQ/JOB/REASON).
# ------------------------------------------------------------------
print("=" * 60)
print("Logistic Regression: Loan Default Prediction")
print("=" * 60)

numeric_features = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ"]
categorical_features = ["JOB", "REASON"]

indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="skip")
    for c in categorical_features
]
encoders = [
    OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe")
    for c in categorical_features
]
assembler = VectorAssembler(
    inputCols=numeric_features + [f"{c}_ohe" for c in categorical_features],
    outputCol="features",
    handleInvalid="skip"
)
lr = LogisticRegression(featuresCol="features", labelCol="BAD")

pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])
model = pipeline.fit(train)

# output out=work.train_scored predicted=pred_prob;
# probability is a 2-element vector [P(BAD=0), P(BAD=1)]; take element 1.
train_scored = model.transform(train) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

lr_model = model.stages[-1]
print("Model fit complete. Coefficients (in assembled feature order):")
print(f"  Intercept: {lr_model.intercept}")
print(f"  Coefficients: {lr_model.coefficients}")

# ------------------------------------------------------------------
# Step 4: Score the validation dataset
# SAS equivalent:
#   proc plm restore=work.logit_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
# (ilink -> predicted probability on the response scale)
# ------------------------------------------------------------------
valid_scored = model.transform(valid) \
    .withColumn("pred_prob", vector_to_array(col("probability"))[1])

# ------------------------------------------------------------------
# Step 5: Create predicted classes
# SAS equivalent:
#   data work.valid_scored;
#       set work.valid_scored;
#       if pred_prob >= 0.5 then PREDICTED_BAD = 1; else PREDICTED_BAD = 0;
#   run;
# ------------------------------------------------------------------
valid_scored = valid_scored.withColumn(
    "PREDICTED_BAD",
    when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
)

# ------------------------------------------------------------------
# Step 5b: Confusion matrix
# SAS equivalent:
#   title "Confusion Matrix - Validation Set";
#   proc freq data=work.valid_scored;
#       tables BAD * PREDICTED_BAD / nopercent norow nocol;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Confusion Matrix - Validation Set")
print("(rows = actual BAD, columns = PREDICTED_BAD)")
print("=" * 60)
valid_scored.crosstab("BAD", "PREDICTED_BAD").orderBy("BAD_PREDICTED_BAD").show()

# ------------------------------------------------------------------
# Step 6: Model performance - concordance and AUC
# SAS equivalent:
#   title "Model Performance - Concordance and AUC";
#   proc logistic data=work.valid descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ JOB REASON;
#       roc;
#       ods output Association=work.association_stats;
#   run;
#
# The SAS c-statistic (area under the ROC curve) equals AUC; we evaluate the
# fitted model's predictions on the validation set with a binary evaluator.
# ------------------------------------------------------------------
auc_evaluator = BinaryClassificationEvaluator(
    labelCol="BAD", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)
auc = auc_evaluator.evaluate(valid_scored)
# Somers' D (Gamma / Tau-a share the same ordering as c); c = (D + 1) / 2.
somers_d = 2 * auc - 1

# ------------------------------------------------------------------
# Step 7: Display key association statistics
# SAS equivalent:
#   title "Model Association Statistics";
#   proc print data=work.association_stats;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Model Association Statistics")
print("=" * 60)
assoc_stats = spark.createDataFrame(
    [
        ("c (Area Under ROC)", round(auc, 4)),
        ("Somers' D", round(somers_d, 4)),
        ("Percent Concordant", round(auc * 100, 2)),
    ],
    ["Statistic", "Value"]
)
assoc_stats.show(truncate=False)

# ------------------------------------------------------------------
# Step 8: Score distribution by actual outcome
# SAS equivalent:
#   title "Predicted Probability Distribution by Actual Outcome";
#   proc means data=work.valid_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Predicted Probability Distribution by Actual Outcome")
print("(equivalent to PROC MEANS with CLASS BAD)")
print("=" * 60)
valid_scored.groupBy("BAD").agg(
    count("pred_prob").alias("N"),
    mean("pred_prob").alias("Mean"),
    stddev("pred_prob").alias("Std"),
    spark_min("pred_prob").alias("Min"),
    expr("percentile_approx(pred_prob, 0.25)").alias("P25"),
    expr("percentile_approx(pred_prob, 0.5)").alias("Median"),
    expr("percentile_approx(pred_prob, 0.75)").alias("P75"),
    spark_max("pred_prob").alias("Max"),
).orderBy("BAD").show(truncate=False)

# Clean up
spark.stop()
