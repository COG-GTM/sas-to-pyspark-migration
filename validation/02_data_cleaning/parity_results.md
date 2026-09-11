# Parity results: 02_data_cleaning

Stage log: `validation/logs/02_data_cleaning.txt`

Row base (from SAS): 5960 raw rows -> `home_equity_filtered` keeps LOAN, VALUE, BAD non-missing (5848 rows) -> `home_equity_final` additionally keeps 0 < LTV < 5, LOAN > 0, VALUE > 0 (5337 rows). PROC MEANS #1 runs on the filtered set, PROC MEANS #2 and PROC PRINT on the final set. Percentiles follow SAS PCTLDEF=5.

**147 / 147 checks matched**

| check | expected (SAS semantics, pandas) | actual (stage log) | tolerance | status |
|---|---|---|---|---|
| rows.original | 5960 | 5960 | exact | MATCH |
| rows.filtered | 5848 | 5848 | exact | MATCH |
| rows.final | 5337 | 5337 | exact | MATCH |
| outlier.LOAN.count | 5848 | 5848 | exact | MATCH |
| outlier.LOAN.nmiss | 0 | 0 | exact | MATCH |
| outlier.LOAN.mean | 18598.08482 | 18598.08482 | rel 1e-6 | MATCH |
| outlier.LOAN.stddev | 11195.20239 | 11195.20239 | rel 1e-6 | MATCH |
| outlier.LOAN.min | 1100 | 1100 | rel 1e-9 | MATCH |
| outlier.LOAN.max | 89900 | 89900 | rel 1e-9 | MATCH |
| pctl.LOAN.p1 | 3600 | 3600 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LOAN.p5 | 5900 | 5900 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LOAN.p25 | 11100 | 11100 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LOAN.median | 16400 | 16400 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LOAN.p75 | 23200 | 23200 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LOAN.p95 | 40000 | 40000 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LOAN.p99 | 62500 | 62500 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| outlier.MORTDUE.count | 5357 | 5357 | exact | MATCH |
| outlier.MORTDUE.nmiss | 491 | 491 | exact | MATCH |
| outlier.MORTDUE.mean | 73762.97222 | 73762.97222 | rel 1e-6 | MATCH |
| outlier.MORTDUE.stddev | 44189.81431 | 44189.81431 | rel 1e-6 | MATCH |
| outlier.MORTDUE.min | 2063 | 2063 | rel 1e-9 | MATCH |
| outlier.MORTDUE.max | 399412 | 399412 | rel 1e-9 | MATCH |
| pctl.MORTDUE.p1 | 8117 | 8117 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.MORTDUE.p5 | 18371 | 18371 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.MORTDUE.p25 | 46466 | 46466 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.MORTDUE.median | 65021 | 65021 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.MORTDUE.p75 | 91350 | 91350 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.MORTDUE.p95 | 152029 | 152029 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.MORTDUE.p99 | 232057 | 232057 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| outlier.VALUE.count | 5848 | 5848 | exact | MATCH |
| outlier.VALUE.nmiss | 0 | 0 | exact | MATCH |
| outlier.VALUE.mean | 101776.0487 | 101776.0487 | rel 1e-6 | MATCH |
| outlier.VALUE.stddev | 57385.77533 | 57385.77533 | rel 1e-6 | MATCH |
| outlier.VALUE.min | 8000 | 8000 | rel 1e-9 | MATCH |
| outlier.VALUE.max | 855909 | 855909 | rel 1e-9 | MATCH |
| pctl.VALUE.p1 | 26140 | 26140 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.VALUE.p5 | 39050 | 39050 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.VALUE.p25 | 66069 | 66056 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.VALUE.median | 89235.5 | 89231 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.VALUE.p75 | 119831.5 | 119817 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.VALUE.p95 | 203720 | 203720 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.VALUE.p99 | 289991 | 289991 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| outlier.DEBTINC.count | 4662 | 4662 | exact | MATCH |
| outlier.DEBTINC.nmiss | 1186 | 1186 | exact | MATCH |
| outlier.DEBTINC.mean | 33.80941834 | 33.80941834 | rel 1e-6 | MATCH |
| outlier.DEBTINC.stddev | 8.564806248 | 8.564806248 | rel 1e-6 | MATCH |
| outlier.DEBTINC.min | 0.7202950067 | 0.7202950067 | rel 1e-9 | MATCH |
| outlier.DEBTINC.max | 203.3121487 | 203.3121487 | rel 1e-9 | MATCH |
| pctl.DEBTINC.p1 | 13.34721268 | 13.34721268 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEBTINC.p5 | 20.58821436 | 20.58821436 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEBTINC.p25 | 29.16004451 | 29.16004451 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEBTINC.median | 34.83434725 | 34.83386481 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEBTINC.p75 | 39.00850811 | 39.00850811 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEBTINC.p95 | 42.73666557 | 42.73666557 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEBTINC.p99 | 49.20639579 | 49.20639579 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| outlier.LTV.count | 5357 | 5357 | exact | MATCH |
| outlier.LTV.nmiss | 491 | 491 | exact | MATCH |
| outlier.LTV.mean | 0.7084935888 | 0.7084935888 | rel 1e-6 | MATCH |
| outlier.LTV.stddev | 0.3811153433 | 0.3811153433 | rel 1e-6 | MATCH |
| outlier.LTV.min | 0.02053798981 | 0.02053798981 | rel 1e-9 | MATCH |
| outlier.LTV.max | 7.1625 | 7.1625 | rel 1e-9 | MATCH |
| pctl.LTV.p1 | 0.1216376197 | 0.1216376197 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LTV.p5 | 0.3161372759 | 0.3161372759 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LTV.p25 | 0.6230004794 | 0.6230004794 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LTV.median | 0.71886121 | 0.71886121 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LTV.p75 | 0.7956138912 | 0.7956138912 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LTV.p95 | 0.8911175499 | 0.8911175499 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.LTV.p99 | 0.9965048544 | 0.9965048544 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| outlier.CLAGE.count | 5559 | 5559 | exact | MATCH |
| outlier.CLAGE.nmiss | 289 | 289 | exact | MATCH |
| outlier.CLAGE.mean | 180.0719771 | 180.0719771 | rel 1e-6 | MATCH |
| outlier.CLAGE.stddev | 86.03260651 | 86.03260651 | rel 1e-6 | MATCH |
| outlier.CLAGE.min | 0 | 0 | rel 1e-9 | MATCH |
| outlier.CLAGE.max | 1168.233561 | 1168.233561 | rel 1e-9 | MATCH |
| pctl.CLAGE.p1 | 29.85636517 | 29.85636517 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.CLAGE.p5 | 68.89009617 | 68.89009617 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.CLAGE.p25 | 115.1302076 | 115.1302076 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.CLAGE.median | 173.6252843 | 173.6252843 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.CLAGE.p75 | 232.2921386 | 232.2921386 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.CLAGE.p95 | 322.0045958 | 322.0045958 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.CLAGE.p99 | 402.0868105 | 402.0868105 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| outlier.DEROG.count | 5168 | 5168 | exact | MATCH |
| outlier.DEROG.nmiss | 680 | 680 | exact | MATCH |
| outlier.DEROG.mean | 0.2409055728 | 0.2409055728 | rel 1e-6 | MATCH |
| outlier.DEROG.stddev | 0.815596061 | 0.815596061 | rel 1e-6 | MATCH |
| outlier.DEROG.min | 0 | 0 | rel 1e-9 | MATCH |
| outlier.DEROG.max | 10 | 10 | rel 1e-9 | MATCH |
| pctl.DEROG.p1 | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEROG.p5 | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEROG.p25 | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEROG.median | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEROG.p75 | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEROG.p95 | 2 | 2 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DEROG.p99 | 4 | 4 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| outlier.DELINQ.count | 5292 | 5292 | exact | MATCH |
| outlier.DELINQ.nmiss | 556 | 556 | exact | MATCH |
| outlier.DELINQ.mean | 0.4232804233 | 0.4232804233 | rel 1e-6 | MATCH |
| outlier.DELINQ.stddev | 1.078286216 | 1.078286216 | rel 1e-6 | MATCH |
| outlier.DELINQ.min | 0 | 0 | rel 1e-9 | MATCH |
| outlier.DELINQ.max | 15 | 15 | rel 1e-9 | MATCH |
| pctl.DELINQ.p1 | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DELINQ.p5 | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DELINQ.p25 | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DELINQ.median | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DELINQ.p75 | 0 | 0 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DELINQ.p95 | 3 | 3 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| pctl.DELINQ.p99 | 5 | 5 | rel 1% (PCTLDEF=5 vs exact rank) | MATCH |
| final.LOAN.count | 5337 | 5337 | exact | MATCH |
| final.LOAN.nmiss | 0 | 0 | exact | MATCH |
| final.LOAN.mean | 18556.34251 | 18556.34251 | rel 1e-6 | MATCH |
| final.LOAN.stddev | 11043.6573 | 11043.6573 | rel 1e-6 | MATCH |
| final.LOAN.min | 1100 | 1100 | rel 1e-9 | MATCH |
| final.LOAN.max | 89900 | 89900 | rel 1e-9 | MATCH |
| final.MORTDUE.count | 5337 | 5337 | exact | MATCH |
| final.MORTDUE.nmiss | 0 | 0 | exact | MATCH |
| final.MORTDUE.mean | 73245.88143 | 73245.88143 | rel 1e-6 | MATCH |
| final.MORTDUE.stddev | 43296.49949 | 43296.49949 | rel 1e-6 | MATCH |
| final.MORTDUE.min | 2063 | 2063 | rel 1e-9 | MATCH |
| final.MORTDUE.max | 399412 | 399412 | rel 1e-9 | MATCH |
| final.VALUE.count | 5337 | 5337 | exact | MATCH |
| final.VALUE.nmiss | 0 | 0 | exact | MATCH |
| final.VALUE.mean | 105255.4187 | 105255.4187 | rel 1e-6 | MATCH |
| final.VALUE.stddev | 54074.82651 | 54074.82651 | rel 1e-6 | MATCH |
| final.VALUE.min | 12737 | 12737 | rel 1e-9 | MATCH |
| final.VALUE.max | 512650 | 512650 | rel 1e-9 | MATCH |
| final.LTV.count | 5337 | 5337 | exact | MATCH |
| final.LTV.nmiss | 0 | 0 | exact | MATCH |
| final.LTV.mean | 0.6889890984 | 0.6889890984 | rel 1e-6 | MATCH |
| final.LTV.stddev | 0.2064954405 | 0.2064954405 | rel 1e-6 | MATCH |
| final.LTV.min | 0.02053798981 | 0.02053798981 | rel 1e-9 | MATCH |
| final.LTV.max | 4.705660674 | 4.705660674 | rel 1e-9 | MATCH |
| final.DEBTINC.count | 4253 | 4253 | exact | MATCH |
| final.DEBTINC.nmiss | 1084 | 1084 | exact | MATCH |
| final.DEBTINC.mean | 34.29305332 | 34.29305332 | rel 1e-6 | MATCH |
| final.DEBTINC.stddev | 8.297819138 | 8.297819138 | rel 1e-6 | MATCH |
| final.DEBTINC.min | 0.8381175254 | 0.8381175254 | rel 1e-9 | MATCH |
| final.DEBTINC.max | 203.3121487 | 203.3121487 | rel 1e-9 | MATCH |
| print.obs1 | 1|1100|25860|39025|0.6626521461|Default|NULL|Other|HomeImp | 1|1100|25860.0|39025.0|0.6626521460602178|Default|NULL|Other|HomeImp | exact per field | MATCH |
| print.obs2 | 1|1300|70053|68400|1.024166667|Default|NULL|Other|HomeImp | 1|1300|70053.0|68400.0|1.0241666666666667|Default|NULL|Other|HomeImp | exact per field | MATCH |
| print.obs3 | 1|1500|13500|16700|0.8083832335|Default|NULL|Other|HomeImp | 1|1500|13500.0|16700.0|0.8083832335329342|Default|NULL|Other|HomeImp | exact per field | MATCH |
| print.obs4 | 0|1700|97800|112000|0.8732142857|Paid|NULL|Office|HomeImp | 0|1700|97800.0|112000.0|0.8732142857142857|Paid|NULL|Office|HomeImp | exact per field | MATCH |
| print.obs5 | 1|1700|30548|40320|0.7576388889|Default|37.11361356|Other|HomeImp | 1|1700|30548.0|40320.0|0.7576388888888889|Default|37.113613558|Other|HomeImp | exact per field | MATCH |
| print.obs6 | 1|1800|48649|57037|0.8529375668|Default|NULL|Other|HomeImp | 1|1800|48649.0|57037.0|0.8529375668425758|Default|NULL|Other|HomeImp | exact per field | MATCH |
| print.obs7 | 1|1800|28502|43034|0.6623135195|Default|36.88489409|Other|HomeImp | 1|1800|28502.0|43034.0|0.6623135195426871|Default|36.884894093|Other|HomeImp | exact per field | MATCH |
| print.obs8 | 1|2000|32700|46740|0.6996148909|Default|NULL|Other|HomeImp | 1|2000|32700.0|46740.0|0.699614890885751|Default|NULL|Other|HomeImp | exact per field | MATCH |
| print.obs9 | 1|2000|20627|29800|0.6921812081|Default|NULL|Office|HomeImp | 1|2000|20627.0|29800.0|0.6921812080536913|Default|NULL|Office|HomeImp | exact per field | MATCH |
| print.obs10 | 1|2000|45000|55000|0.8181818182|Default|NULL|Other|HomeImp | 1|2000|45000.0|55000.0|0.8181818181818182|Default|NULL|Other|HomeImp | exact per field | MATCH |
