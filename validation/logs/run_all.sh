#!/usr/bin/env bash
# Re-runs every migration stage, the pytest suite and every parity check on the merged tree.
# Usage (from repo root): bash validation/logs/run_all.sh
set -u
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
cd "$(git rev-parse --show-toplevel)"
LOGS=validation/logs
mkdir -p "$LOGS"
: > "$LOGS/exit_codes.txt"

for s in 01_data_loading 02_data_cleaning 03_aggregation_reporting 04_risk_segmentation 05_logistic_regression; do
  echo "=== running pyspark/$s.py"
  python "pyspark/$s.py" > "$LOGS/$s.txt" 2> "$LOGS/$s.stderr.txt"
  rc=$?
  echo "$s exit=$rc" | tee -a "$LOGS/exit_codes.txt"
done

echo "=== pytest"
python -m pytest tests/test_pyspark_outputs.py -v > "$LOGS/pytest.txt" 2>&1
echo "pytest exit=$?" | tee -a "$LOGS/exit_codes.txt"

echo "=== parity checks"
python validation/01_data_loading/parity_check.py --log "$LOGS/01_data_loading.txt" > "$LOGS/parity_01_data_loading.txt" 2>&1
echo "parity_01_data_loading exit=$?" | tee -a "$LOGS/exit_codes.txt"
python validation/02_data_cleaning/parity_check.py --log "$LOGS/02_data_cleaning.txt" > "$LOGS/parity_02_data_cleaning.txt" 2>&1
echo "parity_02_data_cleaning exit=$?" | tee -a "$LOGS/exit_codes.txt"
python validation/03_aggregation_reporting/parity_check.py "$LOGS/03_aggregation_reporting.txt" > "$LOGS/parity_03_aggregation_reporting.txt" 2>&1
echo "parity_03_aggregation_reporting exit=$?" | tee -a "$LOGS/exit_codes.txt"
python validation/04_risk_segmentation/parity_check.py --log "$LOGS/04_risk_segmentation.txt" > "$LOGS/parity_04_risk_segmentation.txt" 2>&1
echo "parity_04_risk_segmentation exit=$?" | tee -a "$LOGS/exit_codes.txt"
python validation/05_logistic_regression/parity_check.py --log "$LOGS/05_logistic_regression.txt" > "$LOGS/parity_05_logistic_regression.txt" 2>&1
echo "parity_05_logistic_regression exit=$?" | tee -a "$LOGS/exit_codes.txt"

echo "=== environment"
{
  echo "OS: $(lsb_release -ds)"
  echo "Java: $(java -version 2>&1 | head -1)"
  echo "Python: $(python --version 2>&1)"
  python -c "import pyspark, pandas, sklearn, pytest; print(f'PySpark: {pyspark.__version__}\npandas: {pandas.__version__}\nscikit-learn: {sklearn.__version__}\npytest: {pytest.__version__}')"
  echo "base commit: $(git rev-parse origin/main)"
  echo "merged HEAD: $(git rev-parse HEAD)"
} > "$LOGS/environment.txt"
cat "$LOGS/environment.txt"
