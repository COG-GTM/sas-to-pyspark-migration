# SAS to PySpark Migration Mapping Reference

A comprehensive reference table mapping SAS constructs to their PySpark equivalents.

## Data Access & I/O

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC IMPORT` (CSV) | `spark.read.csv()` | Use `header=True, inferSchema=True` |
| `PROC IMPORT` (Excel) | `spark.read.format("com.crealytics.spark.excel")` | Requires external package |
| `PROC IMPORT` (database) | `spark.read.jdbc()` | Provide JDBC URL and connection properties |
| `PROC EXPORT` (CSV) | `df.write.csv()` | Use `header=True, mode="overwrite"` |
| `PROC EXPORT` (Parquet) | `df.write.parquet()` | Native Spark format, preferred for performance |
| `LIBNAME` (file path) | `spark.read` with path | Configure path or connection string |
| `LIBNAME` (database) | `spark.read.jdbc()` / `spark.read.format()` | Use JDBC or native connectors |

## DATA Step Operations

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `DATA step` (new dataset) | `df.withColumn()`, `df.select()`, `df.filter()` | Chain DataFrame transformations |
| `SET` (read dataset) | Source DataFrame reference | PySpark DataFrames are immutable |
| `IF/THEN/ELSE` | `when().otherwise()` | From `pyspark.sql.functions` |
| `WHERE` clause | `.filter()` / `.where()` | Both are aliases |
| `KEEP` / `DROP` | `.select()` / `.drop()` | Select or drop columns |
| `RENAME` | `.withColumnRenamed()` | Rename one column at a time |
| `LENGTH` statement | `.withColumn(col.cast())` | Cast to appropriate type |
| `FORMAT` / `INFORMAT` | No direct equivalent | Handle in display or write logic |
| `LABEL` | No direct equivalent | Document in metadata dictionary |
| `ARRAY` processing | List comprehension + `withColumn` loop | Iterate over column list |
| `RETAIN` | Window functions with `lag()` | Use `Window.orderBy()` |
| `RETAIN` + sum statement (`CUM + X;`) | `sum("X").over(Window.partitionBy(by).orderBy(sort).rowsBetween(unboundedPreceding, currentRow))` | Running total reset per BY group. Use `rowsBetween`, not `rangeBetween`, so ties on the sort key accumulate one row at a time like the sequential DATA step |
| `SEQ + 1;` counter reset on `first.` | `row_number().over(window)` | Sequence within BY group |
| `FIRST.` / `LAST.` | Window functions with `row_number()` | Partition and order window |
| `IS_FIRST = first.var;` (flag as 1/0) | `(row_number().over(w) == 1).cast("int")` | SAS stores `first.`/`last.` as numeric 1/0 |
| `last.var` | `row_number().over(w) == count("*").over(Window.partitionBy(by))` | Compare position to partition size (or `row_number()` over the reversed order) |
| `if last.var then output;` | `.filter(col("IS_LAST") == 1)` | Conditional `OUTPUT` -> filter after computing flags |
| `BY` group processing | `.groupBy()` | Group-level operations |
| `PROC SORT` stable tie order (default `EQUALS`) | Add `monotonically_increasing_id()` as a final `orderBy` key | Spark window ordering is not stable on ties; capture input order explicitly |
| `MERGE` (join) | `.join()` | Specify join type and condition |
| `OUTPUT` | `.union()` / write operations | Append rows or write results |
| `SET ds1 ds2 ds3;` (stack datasets) | `functools.reduce(DataFrame.unionByName, [df1, df2, df3])` | Column names, not positions, are matched |
| `LENGTH VAR $7;` | Not needed | Spark strings are variable-length; no truncation to declare |
| `missing(X)` | `col("X").isNull()` | Works for numeric and character |
| `upcase()` | `upper()` | From `pyspark.sql.functions` |
| `if X = . then X = 0;` | `coalesce(col("X"), lit(0))` / `na.fill({"X": 0})` | Replace missing with a constant |
| `X in (., 0)` | `col("X").isNull() \| (col("X") == 0)` | `isin()` does not match nulls; test `isNull()` separately |
| `A / B` with `B` missing or 0 | `when(col("B").isNull() \| (col("B") == 0), None).otherwise(col("A") / col("B"))` | SAS yields missing; Spark yields `Infinity` for `/ 0` (or null in ANSI mode), so guard explicitly |
| `propcase()` | `initcap()` | From `pyspark.sql.functions` |
| `substr()` | `substring()` | From `pyspark.sql.functions` |
| `compress()` | `regexp_replace()` | Remove characters with regex |
| `catx()` | `concat_ws()` | Concatenate with separator |
| `input()` | `.cast()` | Type conversion |
| `put()` | `.cast("string")` | Convert to string |
| Missing value (`.`) | `None` / `null` | Use `.isNull()` / `.isNotNull()` |

## Statistical Procedures

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC MEANS` | `.groupBy().agg()` / `.describe()` | Use `mean`, `stddev`, `min`, `max` from functions |
| `PROC SUMMARY` | `.groupBy().agg()` | Same as PROC MEANS equivalent |
| `PROC FREQ` | `.groupBy().count()` / `.crosstab()` | For frequency tables and cross-tabs |
| `PROC TABULATE` | `.groupBy().pivot().agg()` | Pivot for cross-tabulation layout |
| `PROC SQL` | `spark.sql()` | Full SQL support via Spark SQL |
| `PROC SQL` `calculated alias` | `.agg(...)` then `.withColumn("RATE", col("N_BAD") / col("N"))` | Spark SQL cannot reference a same-SELECT alias; compute in a follow-up step (or a subquery/CTE) |
| `sum(cond)` (boolean sums as 1/0) | `sum((cond).cast("int"))` | Spark cannot sum booleans without a cast |
| `select 1 as THRESHOLD` (constant column) | `lit(value).alias("THRESHOLD")` | Constants inside `.agg()` or `.withColumn()` |
| `format=percent8.2`, `dollar12.`, `8.3` on a column | `format_string("%.2f%%", col * 100)`, `format_string("$%s", format_number(col, 0))`, `format_string("%.3f", col)` | SAS formats are display-only; keep stored columns numeric and format at print time |
| `PROC SORT` | `.orderBy()` / `.sort()` | Specify ascending/descending |
| `PROC SORT NODUPKEY` | `.dropDuplicates()` | Deduplicate by specified columns |
| `PROC TRANSPOSE` | `.pivot()` / stack patterns | Wide-to-long or long-to-wide |
| `PROC CONTENTS` | `.printSchema()` / `.dtypes` | Schema and metadata inspection |
| `PROC PRINT` | `.show()` | Display rows |
| `PROC UNIVARIATE` | `.describe()` + `.approxQuantile()` | Combine for full distribution stats |
| `PROC CORR` | `Correlation.corr()` | From `pyspark.ml.stat` |
| `PROC FORMAT` | `when().otherwise()` or UDFs | Map value ranges to labels |

## Machine Learning

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC LOGISTIC` | `pyspark.ml.classification.LogisticRegression` | Use Pipeline with feature preparation |
| `PROC REG` | `pyspark.ml.regression.LinearRegression` | Similar pipeline approach |
| `PROC GLMSELECT` | `pyspark.ml.regression` + `CrossValidator` | Model selection via cross-validation |
| `CLASS` statement | `StringIndexer` + `OneHotEncoder` | Encode categorical variables |
| `MODEL` statement | `VectorAssembler` + ML algorithm | Assemble features, then fit |
| `SELECTION=STEPWISE` | `elasticNetParam` / `CrossValidator` | Regularization for feature selection |
| `OUTPUT PREDICTED=` | `model.transform()` | Produces `prediction` and `probability` columns |
| `PROC SCORE` | `model.transform()` | Apply fitted model to new data |
| `ODS OUTPUT` | Evaluate with `Evaluator` classes | `BinaryClassificationEvaluator`, etc. |
| `PROC CLUSTER` | `pyspark.ml.clustering.KMeans` | K-Means, Bisecting K-Means, GMM |
| `PROC TREE` | `pyspark.ml.classification.DecisionTreeClassifier` | Tree-based classification |
| `PROC FOREST` | `pyspark.ml.classification.RandomForestClassifier` | Ensemble methods |

## Macro Language

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `%LET` macro variable | Python variable | `myVar = "value"` |
| `%MACRO` / `%MEND` | Python function | `def myFunction():` |
| `&macrovar` resolution | f-string / `.format()` | `f"SELECT * FROM {tableName}"` |
| `%IF / %THEN / %ELSE` | Python `if / elif / else` | Standard control flow |
| `%DO` loop | Python `for` loop | `for i in range(n):` |
| `%do i = 1 %to %sysfunc(countw(&list)); %let x = %scan(&list, &i);` | `for x in [1, 2, 3]:` | Space-delimited macro list -> Python list; no `countw`/`%scan` needed |
| `create table work.tbl_&thr` (generated table names) | `{f"tbl_{thr}": df for thr in thresholds}` | Loop outputs keyed in a dict instead of dynamic dataset names |
| `%INCLUDE` | `import` / `exec(open().read())` | Module imports or script execution |
| `%SYSFUNC()` | Python built-in functions | Direct function calls |
| `CALL SYMPUTX()` | Variable assignment | `result = df.collect()[0][0]` |

## Data Management

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC APPEND` | `.union()` / `.unionByName()` | Combine DataFrames vertically |
| `PROC COPY` | `df.write` | Write to new location |
| `PROC DELETE` | `spark.catalog.dropTempView()` | Remove temp views |
| `PROC DATASETS` | Catalog operations | Manage tables in catalog |
| `PROC COMPARE` | Custom comparison logic | Compare two DataFrames |
| Permanent dataset | `.write.saveAsTable()` | Save to Hive metastore |
| Temporary dataset | `.createOrReplaceTempView()` | Session-scoped temp table |
