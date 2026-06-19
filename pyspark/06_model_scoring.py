"""
PySpark Script: 06_model_scoring.py
Purpose: Model evaluation and scoring pipeline
Equivalent SAS Program: sas/06_model_scoring.sas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, mean as spark_mean,
    min as spark_min, max as spark_max, percent_rank, ceil as spark_ceil,
    udf, abs as spark_abs
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HomeEquity_ModelScoring") \
    .master("local[*]") \
    .getOrCreate()

# Load and prepare data (replicate cleaning from scripts 02/04)
df = spark.read.csv("data/home_equity.csv", header=True, inferSchema=True)
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
        (col("LOAN") > 0) & (col("VALUE") > 0)
    )

# ------------------------------------------------------------------
# Step 1: Prepare modeling dataset - remove records with excessive missing
# SAS equivalent:
#   data work.model_data;
#       set work.home_equity_risk;
#       if LOAN ne . and MORTDUE ne . and VALUE ne .
#          and DEBTINC ne . and DELINQ ne . and CLAGE ne .
#          and DEROG ne . and NINQ ne .
#          and JOB ne '' and REASON ne '';
#   run;
# ------------------------------------------------------------------
modelData = df.filter(
    col("LOAN").isNotNull() &
    col("MORTDUE").isNotNull() &
    col("VALUE").isNotNull() &
    col("DEBTINC").isNotNull() &
    col("DELINQ").isNotNull() &
    col("CLAGE").isNotNull() &
    col("DEROG").isNotNull() &
    col("NINQ").isNotNull() &
    col("JOB").isNotNull() &
    col("REASON").isNotNull()
)

modelData = modelData.withColumn("label", col("BAD").cast("double"))
print(f"Modeling dataset size: {modelData.count()} rows")

# ------------------------------------------------------------------
# Step 2: Split into train (60%), validation (20%), holdout (20%)
# SAS equivalent:
#   proc surveyselect ... samprate=0.6 seed=42;   -> train vs remaining
#   proc surveyselect ... samprate=0.5 seed=42;   -> valid vs holdout
# ------------------------------------------------------------------
train, remaining = modelData.randomSplit([0.6, 0.4], seed=42)
valid, holdout = remaining.randomSplit([0.5, 0.5], seed=42)

print(f"Training set:   {train.count()} rows")
print(f"Validation set: {valid.count()} rows")
print(f"Holdout set:    {holdout.count()} rows")

# ------------------------------------------------------------------
# Step 3: Build and fit the ML pipeline
# SAS equivalent:
#   proc logistic data=work.train descending;
#       class JOB(ref='Other') REASON(ref='HomeImp') / param=ref;
#       model BAD = LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CLAGE NINQ
#                   JOB REASON;
#       output out=work.train_scored predicted=pred_prob;
#       store work.scoring_model;
#   run;
# ------------------------------------------------------------------

# Index categorical variables (equivalent to CLASS statement)
jobIndexer = StringIndexer(
    inputCol="JOB", outputCol="JOB_IDX", handleInvalid="keep"
)
reasonIndexer = StringIndexer(
    inputCol="REASON", outputCol="REASON_IDX", handleInvalid="keep"
)

# One-hot encode (equivalent to param=ref)
jobEncoder = OneHotEncoder(inputCol="JOB_IDX", outputCol="JOB_VEC")
reasonEncoder = OneHotEncoder(inputCol="REASON_IDX", outputCol="REASON_VEC")

# Numeric feature columns
numericFeatures = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC",
    "DELINQ", "DEROG", "CLAGE", "NINQ"
]

# Assemble all features into a single vector
assembler = VectorAssembler(
    inputCols=numericFeatures + ["JOB_VEC", "REASON_VEC"],
    outputCol="features"
)

# Logistic regression model
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.8,
    threshold=0.5
)

# Build and fit pipeline
pipeline = Pipeline(stages=[
    jobIndexer, reasonIndexer,
    jobEncoder, reasonEncoder,
    assembler, lr
])

print("\n" + "=" * 60)
print("Training Logistic Regression Model")
print("=" * 60)

model = pipeline.fit(train)

# ------------------------------------------------------------------
# Step 4: Score validation and holdout datasets
# SAS equivalent:
#   proc plm restore=work.scoring_model;
#       score data=work.valid out=work.valid_scored predicted=pred_prob / ilink;
#   run;
#   proc plm restore=work.scoring_model;
#       score data=work.holdout out=work.holdout_scored predicted=pred_prob / ilink;
#   run;
# ------------------------------------------------------------------

extractProb = udf(lambda v: float(v[1]), DoubleType())

validScored = model.transform(valid)
validScored = validScored.withColumn("pred_prob", extractProb(col("probability")))

holdoutScored = model.transform(holdout)
holdoutScored = holdoutScored.withColumn("pred_prob", extractProb(col("probability")))

# ------------------------------------------------------------------
# Step 5: Assign predicted classes using 0.5 threshold
# SAS equivalent:
#   if pred_prob >= 0.5 then PREDICTED_BAD = 1;
#   else PREDICTED_BAD = 0;
# ------------------------------------------------------------------
validScored = validScored.withColumn(
    "PREDICTED_BAD",
    when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
)
holdoutScored = holdoutScored.withColumn(
    "PREDICTED_BAD",
    when(col("pred_prob") >= 0.5, lit(1)).otherwise(lit(0))
)

# ------------------------------------------------------------------
# Step 6: Confusion matrix on holdout
# SAS equivalent:
#   proc freq data=work.holdout_scored;
#       tables BAD * PREDICTED_BAD;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Confusion Matrix - Holdout Set")
print("(equivalent to PROC FREQ tables BAD * PREDICTED_BAD)")
print("=" * 60)

holdoutScored.groupBy("label", "PREDICTED_BAD") \
    .count() \
    .orderBy("label", "PREDICTED_BAD") \
    .show()

# ------------------------------------------------------------------
# Step 7: Compute classification metrics on holdout
# SAS equivalent:
#   proc sql;
#       create table work.holdout_metrics as
#       select ... Accuracy, Precision, Recall ...
#       from work.holdout_scored;
#   quit;
# ------------------------------------------------------------------
print("=" * 60)
print("Holdout Set - Classification Metrics")
print("=" * 60)

# Accuracy
multiEval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction"
)

for metricName in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
    multiEval.setMetricName(metricName)
    value = multiEval.evaluate(holdoutScored)
    print(f"  {metricName}: {value:.4f}")

# ------------------------------------------------------------------
# Step 8: Decile analysis / lift chart
# SAS equivalent:
#   proc rank data=work.holdout_scored out=work.holdout_deciles
#       groups=10 descending;
#       var pred_prob;
#       ranks decile;
#   run;
#   proc sql;
#       create table work.gains_table as ...
#   quit;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Gains Table - Decile Analysis")
print("(equivalent to PROC RANK + PROC SQL gains table)")
print("=" * 60)

# Assign deciles using percent_rank (descending by pred_prob)
windowSpec = Window.orderBy(col("pred_prob").desc())
holdoutDeciles = holdoutScored.withColumn(
    "pct_rank", percent_rank().over(windowSpec)
).withColumn(
    "decile", spark_ceil(col("pct_rank") * 10).cast("int")
).withColumn(
    "decile", when(col("decile") == 0, lit(1)).otherwise(col("decile"))
)

gainsTable = holdoutDeciles.groupBy("decile").agg(
    count("*").alias("N"),
    spark_sum("label").cast("int").alias("Defaults"),
    spark_mean("label").alias("Default_Rate"),
    spark_mean("pred_prob").alias("Avg_Score"),
    spark_min("pred_prob").alias("Min_Score"),
    spark_max("pred_prob").alias("Max_Score")
).orderBy("decile")

gainsTable.show(10, truncate=False)

# ------------------------------------------------------------------
# Step 9: Cumulative gains and KS statistic
# SAS equivalent:
#   proc sql;
#       create table work.cumulative_gains as
#       select ... Cum_Defaults, Cum_N, KS_Diff ...
#   quit;
# ------------------------------------------------------------------
print("=" * 60)
print("Cumulative Gains and KS Analysis")
print("=" * 60)

totalDefaults = holdoutScored.filter(col("label") == 1.0).count()
totalN = holdoutScored.count()

gainsRows = gainsTable.collect()
cumDefaults = 0
cumN = 0
ksMax = 0.0

print(f"{'Decile':>6}  {'N':>5}  {'Defaults':>8}  {'Cum_Def%':>9}  {'Cum_Pop%':>9}  {'KS':>8}")
print("-" * 55)

for row in gainsRows:
    cumDefaults += row["Defaults"]
    cumN += row["N"]
    cumDefPct = cumDefaults / totalDefaults if totalDefaults > 0 else 0
    cumPopPct = cumN / totalN if totalN > 0 else 0
    ksDiff = cumDefPct - cumPopPct
    if abs(ksDiff) > abs(ksMax):
        ksMax = ksDiff
    print(f"{row['decile']:>6}  {row['N']:>5}  {row['Defaults']:>8}  "
          f"{cumDefPct:>8.2%}  {cumPopPct:>8.2%}  {ksDiff:>8.4f}")

print(f"\nKS Statistic: {abs(ksMax):.4f}")

# ------------------------------------------------------------------
# Step 10: AUC on holdout
# SAS equivalent:
#   proc logistic ... roc;
#       ods output Association=work.holdout_assoc;
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Model Performance - AUC / ROC")
print("(equivalent to PROC LOGISTIC ROC analysis)")
print("=" * 60)

binaryEval = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = binaryEval.evaluate(holdoutScored)
print(f"  AUC (Area Under ROC): {auc:.4f}")

binaryEvalPr = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)
aupr = binaryEvalPr.evaluate(holdoutScored)
print(f"  AUPR (Area Under PR Curve): {aupr:.4f}")

gini = 2 * auc - 1
print(f"  Gini Coefficient: {gini:.4f}")

# ------------------------------------------------------------------
# Step 11: Model scorecard - risk grades based on predicted probability
# SAS equivalent:
#   data work.scorecard;
#       set work.holdout_scored;
#       length RISK_GRADE $12;
#       if pred_prob < 0.10 then RISK_GRADE = 'A - Minimal';
#       else if pred_prob < 0.25 then RISK_GRADE = 'B - Low';
#       else if pred_prob < 0.50 then RISK_GRADE = 'C - Moderate';
#       else if pred_prob < 0.75 then RISK_GRADE = 'D - High';
#       else RISK_GRADE = 'E - Critical';
#   run;
# ------------------------------------------------------------------
print("\n" + "=" * 60)
print("Model Scorecard - Risk Grade Distribution")
print("(equivalent to PROC FREQ on RISK_GRADE)")
print("=" * 60)

scorecard = holdoutScored.withColumn(
    "RISK_GRADE",
    when(col("pred_prob") < 0.10, lit("A - Minimal"))
    .when(col("pred_prob") < 0.25, lit("B - Low"))
    .when(col("pred_prob") < 0.50, lit("C - Moderate"))
    .when(col("pred_prob") < 0.75, lit("D - High"))
    .otherwise(lit("E - Critical"))
)

scorecard.groupBy("RISK_GRADE").agg(
    count("*").alias("N"),
    spark_mean("label").alias("Default_Rate"),
    spark_mean("pred_prob").alias("Avg_Pred_Prob"),
    spark_mean("LOAN").alias("Avg_Loan"),
    spark_mean("DEBTINC").alias("Avg_DEBTINC")
).orderBy("RISK_GRADE").show(truncate=False)

# ------------------------------------------------------------------
# Step 12: Predicted probability distribution by actual outcome
# SAS equivalent:
#   proc means data=work.holdout_scored n mean std min p25 median p75 max;
#       class BAD;
#       var pred_prob;
#   run;
# ------------------------------------------------------------------
print("=" * 60)
print("Predicted Probability Distribution by Actual Outcome")
print("=" * 60)

for labelVal in [0.0, 1.0]:
    subset = holdoutScored.filter(col("label") == labelVal)
    print(f"\nActual BAD = {int(labelVal)}:")
    subset.select("pred_prob").describe().show()

# Clean up
spark.stop()
