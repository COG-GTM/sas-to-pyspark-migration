# Parity results: 03_aggregation_reporting

- Log checked: `validation/03_aggregation_reporting/logs/run_before_fixes.txt`
- SAS row base (work.home_equity_final): **5337** rows
- Result: **22 / 245 checks matched**

| Check | Expected (SAS semantics) | Actual (PySpark log) | Status | Note |
|---|---|---|---|---|
| FREQ JOB=Other Frequency | 2070 | 2348 | MISMATCH |  |
| FREQ JOB=Other Percent | 40.1085 | 40.15 | MISMATCH |  |
| FREQ JOB=ProfExe Frequency | 1242 | 1259 | MISMATCH |  |
| FREQ JOB=ProfExe Percent | 24.0651 | 21.53 | MISMATCH |  |
| FREQ JOB=Office Frequency | 871 | 934 | MISMATCH |  |
| FREQ JOB=Office Percent | 16.8766 | 15.97 | MISMATCH |  |
| FREQ JOB=Mgr Frequency | 704 | 749 | MISMATCH |  |
| FREQ JOB=Mgr Percent | 13.6408 | 12.81 | MISMATCH |  |
| FREQ JOB=Self Frequency | 176 | 188 | MISMATCH |  |
| FREQ JOB=Self Percent | 3.4102 | 3.21 | MISMATCH |  |
| FREQ JOB=Sales Frequency | 98 | 108 | MISMATCH |  |
| FREQ JOB=Sales Percent | 1.8989 | 1.85 | MISMATCH |  |
| FREQ JOB missing level excluded from table | no NULL row | NULL row present | MISMATCH |  |
| FREQ JOB Frequency Missing | 176 | MISSING | MISMATCH | statistic not printed |
| FREQ REASON=DebtCon Frequency | 3665 | 3868 | MISMATCH |  |
| FREQ REASON=DebtCon Percent | 70.7802 | 66.14 | MISMATCH |  |
| FREQ REASON=HomeImp Frequency | 1513 | 1748 | MISMATCH |  |
| FREQ REASON=HomeImp Percent | 29.2198 | 29.89 | MISMATCH |  |
| FREQ REASON missing level excluded from table | no NULL row | NULL row present | MISMATCH |  |
| FREQ REASON Frequency Missing | 159 | MISSING | MISMATCH | statistic not printed |
| FREQ LOAN_OUTCOME=Paid Frequency | 4340 | 4764 | MISMATCH |  |
| FREQ LOAN_OUTCOME=Paid Percent | 81.3191 | 81.46 | MISMATCH |  |
| FREQ LOAN_OUTCOME=Default Frequency | 997 | 1084 | MISMATCH |  |
| FREQ LOAN_OUTCOME=Default Percent | 18.6809 | 18.54 | MISMATCH |  |
| FREQ LOAN_OUTCOME missing level excluded from table | no NULL row | no NULL row | MATCH |  |
| FREQ LOAN_OUTCOME Frequency Missing | 0 | MISSING | MISMATCH | statistic not printed |
| FREQ REGION=South Frequency | 2039 | 2225 | MISMATCH |  |
| FREQ REGION=South Percent | 38.205 | 38.05 | MISMATCH |  |
| FREQ REGION=West Frequency | 1266 | 1391 | MISMATCH |  |
| FREQ REGION=West Percent | 23.7212 | 23.79 | MISMATCH |  |
| FREQ REGION=Midwest Frequency | 1112 | 1220 | MISMATCH |  |
| FREQ REGION=Midwest Percent | 20.8357 | 20.86 | MISMATCH |  |
| FREQ REGION=Northeast Frequency | 920 | 1012 | MISMATCH |  |
| FREQ REGION=Northeast Percent | 17.2381 | 17.31 | MISMATCH |  |
| FREQ REGION missing level excluded from table | no NULL row | no NULL row | MATCH |  |
| FREQ REGION Frequency Missing | 0 | MISSING | MISMATCH | statistic not printed |
| MEANS Default LOAN N | 997 | MISSING | MISMATCH | statistic not printed |
| MEANS Default LOAN Mean | 16621.9659 | 16667.16 | MISMATCH |  |
| MEANS Default LOAN Median | 14800 | MISSING | MISMATCH | statistic not printed |
| MEANS Default LOAN Std | 10944.4714 | 11319.73 | MISMATCH |  |
| MEANS Default LOAN Min | 1100 | 1100 | MATCH |  |
| MEANS Default LOAN Max | 77400 | 77400 | MATCH |  |
| MEANS Default MORTDUE N | 997 | MISSING | MISMATCH | statistic not printed |
| MEANS Default MORTDUE Mean | 69092.824 | 69105.76 | MISMATCH |  |
| MEANS Default MORTDUE Median | 60000 | MISSING | MISMATCH | statistic not printed |
| MEANS Default MORTDUE Std | 46498.0801 | MISSING | MISMATCH | statistic not printed |
| MEANS Default MORTDUE Min | 2063 | MISSING | MISMATCH | statistic not printed |
| MEANS Default MORTDUE Max | 399412 | MISSING | MISMATCH | statistic not printed |
| MEANS Default VALUE N | 997 | MISSING | MISMATCH | statistic not printed |
| MEANS Default VALUE Mean | 98876.569 | 98172.85 | MISMATCH |  |
| MEANS Default VALUE Median | 84624 | MISSING | MISMATCH | statistic not printed |
| MEANS Default VALUE Std | 58832.9772 | MISSING | MISMATCH | statistic not printed |
| MEANS Default VALUE Min | 16020 | MISSING | MISMATCH | statistic not printed |
| MEANS Default VALUE Max | 512650 | MISSING | MISMATCH | statistic not printed |
| MEANS Default DEBTINC N | 350 | MISSING | MISMATCH | statistic not printed |
| MEANS Default DEBTINC Mean | 40.5208 | 39.99 | MISMATCH |  |
| MEANS Default DEBTINC Median | 38.3589 | MISSING | MISMATCH | statistic not printed |
| MEANS Default DEBTINC Std | 17.9789 | MISSING | MISMATCH | statistic not printed |
| MEANS Default DEBTINC Min | 0.8381 | MISSING | MISMATCH | statistic not printed |
| MEANS Default DEBTINC Max | 203.3121 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid LOAN N | 4340 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid LOAN Mean | 19000.7143 | 19037.45 | MISMATCH |  |
| MEANS Paid LOAN Median | 16900 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid LOAN Std | 11019.7006 | 11121.14 | MISMATCH |  |
| MEANS Paid LOAN Min | 1700 | 1700 | MATCH |  |
| MEANS Paid LOAN Max | 89900 | 89900 | MATCH |  |
| MEANS Paid MORTDUE N | 4340 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid MORTDUE Mean | 74199.9363 | 74829.25 | MISMATCH |  |
| MEANS Paid MORTDUE Median | 66724 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid MORTDUE Std | 42475.3385 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid MORTDUE Min | 2619 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid MORTDUE Max | 371003 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid VALUE N | 4340 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid VALUE Mean | 106720.7904 | 102595.92 | MISMATCH |  |
| MEANS Paid VALUE Median | 94273 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid VALUE Std | 52819.9195 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid VALUE Min | 12737 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid VALUE Max | 471827 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid DEBTINC N | 3903 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid DEBTINC Mean | 33.7346 | 33.26 | MISMATCH |  |
| MEANS Paid DEBTINC Median | 34.926 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid DEBTINC Std | 6.506 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid DEBTINC Min | 4.03 | MISSING | MISMATCH | statistic not printed |
| MEANS Paid DEBTINC Max | 45.5698 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Mgr REGION=Midwest N | 150 | 157 | MISMATCH |  |
| TABULATE JOB=Mgr REGION=Midwest Default_Rate_Pct | 21.3333 | 21.02 | MISMATCH |  |
| TABULATE JOB=Mgr REGION=Northeast N | 130 | 141 | MISMATCH |  |
| TABULATE JOB=Mgr REGION=Northeast Default_Rate_Pct | 26.9231 | 24.82 | MISMATCH |  |
| TABULATE JOB=Mgr REGION=South N | 265 | 282 | MISMATCH |  |
| TABULATE JOB=Mgr REGION=South Default_Rate_Pct | 19.6226 | 19.5 | MISMATCH |  |
| TABULATE JOB=Mgr REGION=Total N | 704 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Mgr REGION=Total Default_Rate_Pct | 21.875 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Mgr REGION=West N | 159 | 169 | MISMATCH |  |
| TABULATE JOB=Mgr REGION=West Default_Rate_Pct | 22.0126 | 22.49 | MISMATCH |  |
| TABULATE JOB=Office REGION=Midwest N | 195 | 211 | MISMATCH |  |
| TABULATE JOB=Office REGION=Midwest Default_Rate_Pct | 14.8718 | 14.22 | MISMATCH |  |
| TABULATE JOB=Office REGION=Northeast N | 145 | 160 | MISMATCH |  |
| TABULATE JOB=Office REGION=Northeast Default_Rate_Pct | 8.9655 | 8.13 | MISMATCH |  |
| TABULATE JOB=Office REGION=South N | 324 | 352 | MISMATCH |  |
| TABULATE JOB=Office REGION=South Default_Rate_Pct | 12.3457 | 12.5 | MISMATCH |  |
| TABULATE JOB=Office REGION=Total N | 871 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Office REGION=Total Default_Rate_Pct | 12.1699 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Office REGION=West N | 207 | 211 | MISMATCH |  |
| TABULATE JOB=Office REGION=West Default_Rate_Pct | 11.5942 | 11.37 | MISMATCH |  |
| TABULATE JOB=Other REGION=Midwest N | 399 | 454 | MISMATCH |  |
| TABULATE JOB=Other REGION=Midwest Default_Rate_Pct | 20.5514 | 20.93 | MISMATCH |  |
| TABULATE JOB=Other REGION=Northeast N | 365 | 416 | MISMATCH |  |
| TABULATE JOB=Other REGION=Northeast Default_Rate_Pct | 21.9178 | 22.6 | MISMATCH |  |
| TABULATE JOB=Other REGION=South N | 801 | 896 | MISMATCH |  |
| TABULATE JOB=Other REGION=South Default_Rate_Pct | 20.5993 | 20.65 | MISMATCH |  |
| TABULATE JOB=Other REGION=Total N | 2070 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Other REGION=Total Default_Rate_Pct | 21.9324 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Other REGION=West N | 505 | 582 | MISMATCH |  |
| TABULATE JOB=Other REGION=West Default_Rate_Pct | 25.1485 | 24.05 | MISMATCH |  |
| TABULATE JOB=ProfExe REGION=Midwest N | 272 | 277 | MISMATCH |  |
| TABULATE JOB=ProfExe REGION=Midwest Default_Rate_Pct | 13.9706 | 14.44 | MISMATCH |  |
| TABULATE JOB=ProfExe REGION=Northeast N | 212 | 213 | MISMATCH |  |
| TABULATE JOB=ProfExe REGION=Northeast Default_Rate_Pct | 13.2075 | 13.15 | MISMATCH |  |
| TABULATE JOB=ProfExe REGION=South N | 470 | 476 | MISMATCH |  |
| TABULATE JOB=ProfExe REGION=South Default_Rate_Pct | 16.383 | 17.02 | MISMATCH |  |
| TABULATE JOB=ProfExe REGION=Total N | 1242 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=ProfExe REGION=Total Default_Rate_Pct | 15.1369 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=ProfExe REGION=West N | 288 | 293 | MISMATCH |  |
| TABULATE JOB=ProfExe REGION=West Default_Rate_Pct | 15.625 | 15.7 | MISMATCH |  |
| TABULATE JOB=Sales REGION=Midwest N | 20 | 22 | MISMATCH |  |
| TABULATE JOB=Sales REGION=Midwest Default_Rate_Pct | 25 | 22.73 | MISMATCH |  |
| TABULATE JOB=Sales REGION=Northeast N | 17 | 20 | MISMATCH |  |
| TABULATE JOB=Sales REGION=Northeast Default_Rate_Pct | 29.4118 | 30.0 | MISMATCH |  |
| TABULATE JOB=Sales REGION=South N | 38 | 42 | MISMATCH |  |
| TABULATE JOB=Sales REGION=South Default_Rate_Pct | 39.4737 | 35.71 | MISMATCH |  |
| TABULATE JOB=Sales REGION=Total N | 98 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Sales REGION=Total Default_Rate_Pct | 36.7347 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Sales REGION=West N | 23 | 24 | MISMATCH |  |
| TABULATE JOB=Sales REGION=West Default_Rate_Pct | 47.8261 | 45.83 | MISMATCH |  |
| TABULATE JOB=Self REGION=Midwest N | 37 | 39 | MISMATCH |  |
| TABULATE JOB=Self REGION=Midwest Default_Rate_Pct | 21.6216 | 20.51 | MISMATCH |  |
| TABULATE JOB=Self REGION=Northeast N | 27 | 27 | MATCH |  |
| TABULATE JOB=Self REGION=Northeast Default_Rate_Pct | 33.3333 | 33.33 | MATCH |  |
| TABULATE JOB=Self REGION=South N | 70 | 75 | MISMATCH |  |
| TABULATE JOB=Self REGION=South Default_Rate_Pct | 41.4286 | 41.33 | MISMATCH |  |
| TABULATE JOB=Self REGION=Total N | 176 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Self REGION=Total Default_Rate_Pct | 28.9773 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Self REGION=West N | 42 | 47 | MISMATCH |  |
| TABULATE JOB=Self REGION=West Default_Rate_Pct | 11.9048 | 10.64 | MISMATCH |  |
| TABULATE JOB=Total REGION=Midwest N | 1073 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Total REGION=Midwest Default_Rate_Pct | 18.0801 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Total REGION=Northeast N | 896 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Total REGION=Northeast Default_Rate_Pct | 18.9732 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Total REGION=South N | 1968 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Total REGION=South Default_Rate_Pct | 19.2073 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Total REGION=Total N | 5161 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Total REGION=Total Default_Rate_Pct | 19.163 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Total REGION=West N | 1224 | MISSING | MISMATCH | statistic not printed |
| TABULATE JOB=Total REGION=West Default_Rate_Pct | 20.1797 | MISSING | MISMATCH | statistic not printed |
| TABULATE missing JOB class rows excluded | no NULL JOB rows | NULL JOB rows present | MISMATCH |  |
| SQL top-10 state order | Vermont, Rhode Island, Nebraska, Arkansas, Alaska, Delaware, District of Columbia, South Dakota, Connecticut, Pennsylvania | Vermont, Rhode Island, Nebraska, Arkansas, Alaska, South Dakota, Delaware, District of Columbia, Minnesota, Utah | MISMATCH |  |
| SQL Vermont num_loans | 11 | 12 | MISMATCH |  |
| SQL Vermont avg_loan | 25672.7273 | 27283.33 | MISMATCH |  |
| SQL Vermont avg_property_value | 116467.2727 | 113151.08 | MISMATCH |  |
| SQL Vermont default_rate_pct | 9.0909 | 8.33 | MISMATCH |  |
| SQL Rhode Island num_loans | 19 | 20 | MISMATCH |  |
| SQL Rhode Island avg_loan | 23557.8947 | 22895.0 | MISMATCH |  |
| SQL Rhode Island avg_property_value | 116219.6316 | 112961.3 | MISMATCH |  |
| SQL Rhode Island default_rate_pct | 26.3158 | 25.0 | MISMATCH |  |
| SQL Nebraska num_loans | 33 | 35 | MISMATCH |  |
| SQL Nebraska avg_loan | 22106.0606 | 21840.0 | MISMATCH |  |
| SQL Nebraska avg_property_value | 118060.8485 | 113947.69 | MISMATCH |  |
| SQL Nebraska default_rate_pct | 12.1212 | 11.43 | MISMATCH |  |
| SQL Arkansas num_loans | 47 | 54 | MISMATCH |  |
| SQL Arkansas avg_loan | 21444.6809 | 21246.3 | MISMATCH |  |
| SQL Arkansas avg_property_value | 102673.7234 | 95607.74 | MISMATCH |  |
| SQL Arkansas default_rate_pct | 14.8936 | 12.96 | MISMATCH |  |
| SQL Alaska num_loans | 13 | 13 | MATCH |  |
| SQL Alaska avg_loan | 21215.3846 | 21215.38 | MATCH |  |
| SQL Alaska avg_property_value | 106543.2308 | 106543.23 | MATCH |  |
| SQL Alaska default_rate_pct | 7.6923 | 7.69 | MATCH |  |
| SQL Delaware num_loans | 18 | 18 | MATCH |  |
| SQL Delaware avg_loan | 20638.8889 | 20638.89 | MATCH |  |
| SQL Delaware avg_property_value | 97622.0556 | 97622.06 | MATCH |  |
| SQL Delaware default_rate_pct | 27.7778 | 27.78 | MATCH |  |
| SQL District of Columbia num_loans | 12 | 12 | MATCH |  |
| SQL District of Columbia avg_loan | 20575 | 20575.0 | MATCH |  |
| SQL District of Columbia avg_property_value | 95766.75 | 95766.75 | MATCH |  |
| SQL District of Columbia default_rate_pct | 16.6667 | 16.67 | MATCH |  |
| SQL South Dakota num_loans | 15 | 16 | MISMATCH |  |
| SQL South Dakota avg_loan | 20406.6667 | 20762.5 | MISMATCH |  |
| SQL South Dakota avg_property_value | 115230.5333 | 110295.94 | MISMATCH |  |
| SQL South Dakota default_rate_pct | 13.3333 | 12.5 | MISMATCH |  |
| SQL Connecticut num_loans | 56 | MISSING | MISMATCH | statistic not printed |
| SQL Connecticut avg_loan | 19894.6429 | MISSING | MISMATCH | statistic not printed |
| SQL Connecticut avg_property_value | 98450.1071 | MISSING | MISMATCH | statistic not printed |
| SQL Connecticut default_rate_pct | 32.1429 | MISSING | MISMATCH | statistic not printed |
| SQL Pennsylvania num_loans | 207 | MISSING | MISMATCH | statistic not printed |
| SQL Pennsylvania avg_loan | 19728.9855 | MISSING | MISMATCH | statistic not printed |
| SQL Pennsylvania avg_property_value | 110177.4302 | MISSING | MISMATCH | statistic not printed |
| SQL Pennsylvania default_rate_pct | 18.8406 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Default LOAN N | 655 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Default LOAN Mean | 18293.1298 | 18633.14 | MISMATCH |  |
| STEP5 DebtCon/Default LOAN Std | 10172.6006 | 10698.35 | MISMATCH |  |
| STEP5 DebtCon/Default LOAN Median | 16000 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Default LTV N | 655 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Default LTV Mean | 0.7009 | 0.7098 | MISMATCH |  |
| STEP5 DebtCon/Default LTV Std | 0.1698 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Default LTV Median | 0.7241 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Default DEBTINC N | 241 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Default DEBTINC Mean | 40.8511 | 40.55 | MISMATCH |  |
| STEP5 DebtCon/Default DEBTINC Std | 17.2004 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Default DEBTINC Median | 39.1401 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Paid LOAN N | 3010 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Paid LOAN Mean | 19964.4186 | 20176.37 | MISMATCH |  |
| STEP5 DebtCon/Paid LOAN Std | 10456.5568 | 10392.43 | MISMATCH |  |
| STEP5 DebtCon/Paid LOAN Median | 18050 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Paid LTV N | 3010 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Paid LTV Mean | 0.698 | 0.7305 | MISMATCH |  |
| STEP5 DebtCon/Paid LTV Std | 0.2139 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Paid LTV Median | 0.7181 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Paid DEBTINC N | 2705 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Paid DEBTINC Mean | 33.9609 | 33.75 | MISMATCH |  |
| STEP5 DebtCon/Paid DEBTINC Std | 6.3842 | MISSING | MISMATCH | statistic not printed |
| STEP5 DebtCon/Paid DEBTINC Median | 35.1882 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Default LOAN N | 310 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Default LOAN Mean | 12839.3548 | 12638.46 | MISMATCH |  |
| STEP5 HomeImp/Default LOAN Std | 11635.9642 | 11473.52 | MISMATCH |  |
| STEP5 HomeImp/Default LOAN Median | 10000 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Default LTV N | 310 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Default LTV Mean | 0.6688 | 0.6688 | MATCH |  |
| STEP5 HomeImp/Default LTV Std | 0.2059 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Default LTV Median | 0.7107 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Default DEBTINC N | 100 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Default DEBTINC Mean | 38.4888 | 38.34 | MISMATCH |  |
| STEP5 HomeImp/Default DEBTINC Std | 17.3739 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Default DEBTINC Median | 36.3683 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Paid LOAN N | 1203 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Paid LOAN Mean | 16848.6284 | 16946.82 | MISMATCH |  |
| STEP5 HomeImp/Paid LOAN Std | 12409.8401 | 12770.36 | MISMATCH |  |
| STEP5 HomeImp/Paid LOAN Median | 13600 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Paid LTV N | 1203 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Paid LTV Mean | 0.6717 | 0.6717 | MATCH |  |
| STEP5 HomeImp/Paid LTV Std | 0.2058 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Paid LTV Median | 0.7256 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Paid DEBTINC N | 1085 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Paid DEBTINC Mean | 33.2241 | 32.87 | MISMATCH |  |
| STEP5 HomeImp/Paid DEBTINC Std | 6.8738 | MISSING | MISMATCH | statistic not printed |
| STEP5 HomeImp/Paid DEBTINC Median | 34.3862 | MISSING | MISMATCH | statistic not printed |
| STEP5 missing REASON class rows excluded | no NULL REASON rows | NULL REASON rows present | MISMATCH |  |
