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
| `FIRST.` / `LAST.` | Window functions with `row_number()` | Partition and order window |
| `BY` group processing | `.groupBy()` | Group-level operations |
| `MERGE` (join) | `.join()` | Specify join type and condition |
| `OUTPUT` | `.union()` / write operations | Append rows or write results |
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
