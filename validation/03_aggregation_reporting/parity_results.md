# Parity results: 03_aggregation_reporting

- Log checked: `validation/logs/03_aggregation_reporting.txt`
- SAS row base (work.home_equity_final): **5337** rows
- Result: **245 / 245 checks matched**

| Check | Expected (SAS semantics) | Actual (PySpark log) | Status | Note |
|---|---|---|---|---|
| FREQ JOB=Other Frequency | 2070 | 2070 | MATCH |  |
| FREQ JOB=Other Percent | 40.1085 | 40.11 | MATCH |  |
| FREQ JOB=ProfExe Frequency | 1242 | 1242 | MATCH |  |
| FREQ JOB=ProfExe Percent | 24.0651 | 24.07 | MATCH |  |
| FREQ JOB=Office Frequency | 871 | 871 | MATCH |  |
| FREQ JOB=Office Percent | 16.8766 | 16.88 | MATCH |  |
| FREQ JOB=Mgr Frequency | 704 | 704 | MATCH |  |
| FREQ JOB=Mgr Percent | 13.6408 | 13.64 | MATCH |  |
| FREQ JOB=Self Frequency | 176 | 176 | MATCH |  |
| FREQ JOB=Self Percent | 3.4102 | 3.41 | MATCH |  |
| FREQ JOB=Sales Frequency | 98 | 98 | MATCH |  |
| FREQ JOB=Sales Percent | 1.8989 | 1.9 | MATCH |  |
| FREQ JOB missing level excluded from table | no NULL row | no NULL row | MATCH |  |
| FREQ JOB Frequency Missing | 176 | 176 | MATCH |  |
| FREQ REASON=DebtCon Frequency | 3665 | 3665 | MATCH |  |
| FREQ REASON=DebtCon Percent | 70.7802 | 70.78 | MATCH |  |
| FREQ REASON=HomeImp Frequency | 1513 | 1513 | MATCH |  |
| FREQ REASON=HomeImp Percent | 29.2198 | 29.22 | MATCH |  |
| FREQ REASON missing level excluded from table | no NULL row | no NULL row | MATCH |  |
| FREQ REASON Frequency Missing | 159 | 159 | MATCH |  |
| FREQ LOAN_OUTCOME=Paid Frequency | 4340 | 4340 | MATCH |  |
| FREQ LOAN_OUTCOME=Paid Percent | 81.3191 | 81.32 | MATCH |  |
| FREQ LOAN_OUTCOME=Default Frequency | 997 | 997 | MATCH |  |
| FREQ LOAN_OUTCOME=Default Percent | 18.6809 | 18.68 | MATCH |  |
| FREQ LOAN_OUTCOME missing level excluded from table | no NULL row | no NULL row | MATCH |  |
| FREQ LOAN_OUTCOME Frequency Missing | 0 | 0 | MATCH |  |
| FREQ REGION=South Frequency | 2039 | 2039 | MATCH |  |
| FREQ REGION=South Percent | 38.205 | 38.2 | MATCH |  |
| FREQ REGION=West Frequency | 1266 | 1266 | MATCH |  |
| FREQ REGION=West Percent | 23.7212 | 23.72 | MATCH |  |
| FREQ REGION=Midwest Frequency | 1112 | 1112 | MATCH |  |
| FREQ REGION=Midwest Percent | 20.8357 | 20.84 | MATCH |  |
| FREQ REGION=Northeast Frequency | 920 | 920 | MATCH |  |
| FREQ REGION=Northeast Percent | 17.2381 | 17.24 | MATCH |  |
| FREQ REGION missing level excluded from table | no NULL row | no NULL row | MATCH |  |
| FREQ REGION Frequency Missing | 0 | 0 | MATCH |  |
| MEANS Default LOAN N | 997 | 997 | MATCH |  |
| MEANS Default LOAN Mean | 16621.9659 | 16621.97 | MATCH |  |
| MEANS Default LOAN Median | 14800 | 14800.0 | MATCH |  |
| MEANS Default LOAN Std | 10944.4714 | 10944.47 | MATCH |  |
| MEANS Default LOAN Min | 1100 | 1100.0 | MATCH |  |
| MEANS Default LOAN Max | 77400 | 77400.0 | MATCH |  |
| MEANS Default MORTDUE N | 997 | 997 | MATCH |  |
| MEANS Default MORTDUE Mean | 69092.824 | 69092.82 | MATCH |  |
| MEANS Default MORTDUE Median | 60000 | 60000.0 | MATCH |  |
| MEANS Default MORTDUE Std | 46498.0801 | 46498.08 | MATCH |  |
| MEANS Default MORTDUE Min | 2063 | 2063.0 | MATCH |  |
| MEANS Default MORTDUE Max | 399412 | 399412.0 | MATCH |  |
| MEANS Default VALUE N | 997 | 997 | MATCH |  |
| MEANS Default VALUE Mean | 98876.569 | 98876.57 | MATCH |  |
| MEANS Default VALUE Median | 84624 | 84624.0 | MATCH |  |
| MEANS Default VALUE Std | 58832.9772 | 58832.98 | MATCH |  |
| MEANS Default VALUE Min | 16020 | 16020.0 | MATCH |  |
| MEANS Default VALUE Max | 512650 | 512650.0 | MATCH |  |
| MEANS Default DEBTINC N | 350 | 350 | MATCH |  |
| MEANS Default DEBTINC Mean | 40.5208 | 40.52 | MATCH |  |
| MEANS Default DEBTINC Median | 38.3589 | 38.36 | MATCH |  |
| MEANS Default DEBTINC Std | 17.9789 | 17.98 | MATCH |  |
| MEANS Default DEBTINC Min | 0.8381 | 0.84 | MATCH |  |
| MEANS Default DEBTINC Max | 203.3121 | 203.31 | MATCH |  |
| MEANS Paid LOAN N | 4340 | 4340 | MATCH |  |
| MEANS Paid LOAN Mean | 19000.7143 | 19000.71 | MATCH |  |
| MEANS Paid LOAN Median | 16900 | 16900.0 | MATCH |  |
| MEANS Paid LOAN Std | 11019.7006 | 11019.7 | MATCH |  |
| MEANS Paid LOAN Min | 1700 | 1700.0 | MATCH |  |
| MEANS Paid LOAN Max | 89900 | 89900.0 | MATCH |  |
| MEANS Paid MORTDUE N | 4340 | 4340 | MATCH |  |
| MEANS Paid MORTDUE Mean | 74199.9363 | 74199.94 | MATCH |  |
| MEANS Paid MORTDUE Median | 66724 | 66724.0 | MATCH |  |
| MEANS Paid MORTDUE Std | 42475.3385 | 42475.34 | MATCH |  |
| MEANS Paid MORTDUE Min | 2619 | 2619.0 | MATCH |  |
| MEANS Paid MORTDUE Max | 371003 | 371003.0 | MATCH |  |
| MEANS Paid VALUE N | 4340 | 4340 | MATCH |  |
| MEANS Paid VALUE Mean | 106720.7904 | 106720.79 | MATCH |  |
| MEANS Paid VALUE Median | 94273 | 94273.0 | MATCH |  |
| MEANS Paid VALUE Std | 52819.9195 | 52819.92 | MATCH |  |
| MEANS Paid VALUE Min | 12737 | 12737.0 | MATCH |  |
| MEANS Paid VALUE Max | 471827 | 471827.0 | MATCH |  |
| MEANS Paid DEBTINC N | 3903 | 3903 | MATCH |  |
| MEANS Paid DEBTINC Mean | 33.7346 | 33.73 | MATCH |  |
| MEANS Paid DEBTINC Median | 34.926 | 34.93 | MATCH |  |
| MEANS Paid DEBTINC Std | 6.506 | 6.51 | MATCH |  |
| MEANS Paid DEBTINC Min | 4.03 | 4.03 | MATCH |  |
| MEANS Paid DEBTINC Max | 45.5698 | 45.57 | MATCH |  |
| TABULATE JOB=Mgr REGION=Midwest N | 150 | 150 | MATCH |  |
| TABULATE JOB=Mgr REGION=Midwest Default_Rate_Pct | 21.3333 | 21.33 | MATCH |  |
| TABULATE JOB=Mgr REGION=Northeast N | 130 | 130 | MATCH |  |
| TABULATE JOB=Mgr REGION=Northeast Default_Rate_Pct | 26.9231 | 26.92 | MATCH |  |
| TABULATE JOB=Mgr REGION=South N | 265 | 265 | MATCH |  |
| TABULATE JOB=Mgr REGION=South Default_Rate_Pct | 19.6226 | 19.62 | MATCH |  |
| TABULATE JOB=Mgr REGION=Total N | 704 | 704 | MATCH |  |
| TABULATE JOB=Mgr REGION=Total Default_Rate_Pct | 21.875 | 21.88 | MATCH |  |
| TABULATE JOB=Mgr REGION=West N | 159 | 159 | MATCH |  |
| TABULATE JOB=Mgr REGION=West Default_Rate_Pct | 22.0126 | 22.01 | MATCH |  |
| TABULATE JOB=Office REGION=Midwest N | 195 | 195 | MATCH |  |
| TABULATE JOB=Office REGION=Midwest Default_Rate_Pct | 14.8718 | 14.87 | MATCH |  |
| TABULATE JOB=Office REGION=Northeast N | 145 | 145 | MATCH |  |
| TABULATE JOB=Office REGION=Northeast Default_Rate_Pct | 8.9655 | 8.97 | MATCH |  |
| TABULATE JOB=Office REGION=South N | 324 | 324 | MATCH |  |
| TABULATE JOB=Office REGION=South Default_Rate_Pct | 12.3457 | 12.35 | MATCH |  |
| TABULATE JOB=Office REGION=Total N | 871 | 871 | MATCH |  |
| TABULATE JOB=Office REGION=Total Default_Rate_Pct | 12.1699 | 12.17 | MATCH |  |
| TABULATE JOB=Office REGION=West N | 207 | 207 | MATCH |  |
| TABULATE JOB=Office REGION=West Default_Rate_Pct | 11.5942 | 11.59 | MATCH |  |
| TABULATE JOB=Other REGION=Midwest N | 399 | 399 | MATCH |  |
| TABULATE JOB=Other REGION=Midwest Default_Rate_Pct | 20.5514 | 20.55 | MATCH |  |
| TABULATE JOB=Other REGION=Northeast N | 365 | 365 | MATCH |  |
| TABULATE JOB=Other REGION=Northeast Default_Rate_Pct | 21.9178 | 21.92 | MATCH |  |
| TABULATE JOB=Other REGION=South N | 801 | 801 | MATCH |  |
| TABULATE JOB=Other REGION=South Default_Rate_Pct | 20.5993 | 20.6 | MATCH |  |
| TABULATE JOB=Other REGION=Total N | 2070 | 2070 | MATCH |  |
| TABULATE JOB=Other REGION=Total Default_Rate_Pct | 21.9324 | 21.93 | MATCH |  |
| TABULATE JOB=Other REGION=West N | 505 | 505 | MATCH |  |
| TABULATE JOB=Other REGION=West Default_Rate_Pct | 25.1485 | 25.15 | MATCH |  |
| TABULATE JOB=ProfExe REGION=Midwest N | 272 | 272 | MATCH |  |
| TABULATE JOB=ProfExe REGION=Midwest Default_Rate_Pct | 13.9706 | 13.97 | MATCH |  |
| TABULATE JOB=ProfExe REGION=Northeast N | 212 | 212 | MATCH |  |
| TABULATE JOB=ProfExe REGION=Northeast Default_Rate_Pct | 13.2075 | 13.21 | MATCH |  |
| TABULATE JOB=ProfExe REGION=South N | 470 | 470 | MATCH |  |
| TABULATE JOB=ProfExe REGION=South Default_Rate_Pct | 16.383 | 16.38 | MATCH |  |
| TABULATE JOB=ProfExe REGION=Total N | 1242 | 1242 | MATCH |  |
| TABULATE JOB=ProfExe REGION=Total Default_Rate_Pct | 15.1369 | 15.14 | MATCH |  |
| TABULATE JOB=ProfExe REGION=West N | 288 | 288 | MATCH |  |
| TABULATE JOB=ProfExe REGION=West Default_Rate_Pct | 15.625 | 15.63 | MATCH |  |
| TABULATE JOB=Sales REGION=Midwest N | 20 | 20 | MATCH |  |
| TABULATE JOB=Sales REGION=Midwest Default_Rate_Pct | 25 | 25.0 | MATCH |  |
| TABULATE JOB=Sales REGION=Northeast N | 17 | 17 | MATCH |  |
| TABULATE JOB=Sales REGION=Northeast Default_Rate_Pct | 29.4118 | 29.41 | MATCH |  |
| TABULATE JOB=Sales REGION=South N | 38 | 38 | MATCH |  |
| TABULATE JOB=Sales REGION=South Default_Rate_Pct | 39.4737 | 39.47 | MATCH |  |
| TABULATE JOB=Sales REGION=Total N | 98 | 98 | MATCH |  |
| TABULATE JOB=Sales REGION=Total Default_Rate_Pct | 36.7347 | 36.73 | MATCH |  |
| TABULATE JOB=Sales REGION=West N | 23 | 23 | MATCH |  |
| TABULATE JOB=Sales REGION=West Default_Rate_Pct | 47.8261 | 47.83 | MATCH |  |
| TABULATE JOB=Self REGION=Midwest N | 37 | 37 | MATCH |  |
| TABULATE JOB=Self REGION=Midwest Default_Rate_Pct | 21.6216 | 21.62 | MATCH |  |
| TABULATE JOB=Self REGION=Northeast N | 27 | 27 | MATCH |  |
| TABULATE JOB=Self REGION=Northeast Default_Rate_Pct | 33.3333 | 33.33 | MATCH |  |
| TABULATE JOB=Self REGION=South N | 70 | 70 | MATCH |  |
| TABULATE JOB=Self REGION=South Default_Rate_Pct | 41.4286 | 41.43 | MATCH |  |
| TABULATE JOB=Self REGION=Total N | 176 | 176 | MATCH |  |
| TABULATE JOB=Self REGION=Total Default_Rate_Pct | 28.9773 | 28.98 | MATCH |  |
| TABULATE JOB=Self REGION=West N | 42 | 42 | MATCH |  |
| TABULATE JOB=Self REGION=West Default_Rate_Pct | 11.9048 | 11.9 | MATCH |  |
| TABULATE JOB=Total REGION=Midwest N | 1073 | 1073 | MATCH |  |
| TABULATE JOB=Total REGION=Midwest Default_Rate_Pct | 18.0801 | 18.08 | MATCH |  |
| TABULATE JOB=Total REGION=Northeast N | 896 | 896 | MATCH |  |
| TABULATE JOB=Total REGION=Northeast Default_Rate_Pct | 18.9732 | 18.97 | MATCH |  |
| TABULATE JOB=Total REGION=South N | 1968 | 1968 | MATCH |  |
| TABULATE JOB=Total REGION=South Default_Rate_Pct | 19.2073 | 19.21 | MATCH |  |
| TABULATE JOB=Total REGION=Total N | 5161 | 5161 | MATCH |  |
| TABULATE JOB=Total REGION=Total Default_Rate_Pct | 19.163 | 19.16 | MATCH |  |
| TABULATE JOB=Total REGION=West N | 1224 | 1224 | MATCH |  |
| TABULATE JOB=Total REGION=West Default_Rate_Pct | 20.1797 | 20.18 | MATCH |  |
| TABULATE missing JOB class rows excluded | no NULL JOB rows | no NULL JOB rows | MATCH |  |
| SQL top-10 state order | Vermont, Rhode Island, Nebraska, Arkansas, Alaska, Delaware, District of Columbia, South Dakota, Connecticut, Pennsylvania | Vermont, Rhode Island, Nebraska, Arkansas, Alaska, Delaware, District of Columbia, South Dakota, Connecticut, Pennsylvania | MATCH |  |
| SQL Vermont num_loans | 11 | 11 | MATCH |  |
| SQL Vermont avg_loan | 25672.7273 | 25672.73 | MATCH |  |
| SQL Vermont avg_property_value | 116467.2727 | 116467.27 | MATCH |  |
| SQL Vermont default_rate_pct | 9.0909 | 9.09 | MATCH |  |
| SQL Rhode Island num_loans | 19 | 19 | MATCH |  |
| SQL Rhode Island avg_loan | 23557.8947 | 23557.89 | MATCH |  |
| SQL Rhode Island avg_property_value | 116219.6316 | 116219.63 | MATCH |  |
| SQL Rhode Island default_rate_pct | 26.3158 | 26.32 | MATCH |  |
| SQL Nebraska num_loans | 33 | 33 | MATCH |  |
| SQL Nebraska avg_loan | 22106.0606 | 22106.06 | MATCH |  |
| SQL Nebraska avg_property_value | 118060.8485 | 118060.85 | MATCH |  |
| SQL Nebraska default_rate_pct | 12.1212 | 12.12 | MATCH |  |
| SQL Arkansas num_loans | 47 | 47 | MATCH |  |
| SQL Arkansas avg_loan | 21444.6809 | 21444.68 | MATCH |  |
| SQL Arkansas avg_property_value | 102673.7234 | 102673.72 | MATCH |  |
| SQL Arkansas default_rate_pct | 14.8936 | 14.89 | MATCH |  |
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
| SQL South Dakota num_loans | 15 | 15 | MATCH |  |
| SQL South Dakota avg_loan | 20406.6667 | 20406.67 | MATCH |  |
| SQL South Dakota avg_property_value | 115230.5333 | 115230.53 | MATCH |  |
| SQL South Dakota default_rate_pct | 13.3333 | 13.33 | MATCH |  |
| SQL Connecticut num_loans | 56 | 56 | MATCH |  |
| SQL Connecticut avg_loan | 19894.6429 | 19894.64 | MATCH |  |
| SQL Connecticut avg_property_value | 98450.1071 | 98450.11 | MATCH |  |
| SQL Connecticut default_rate_pct | 32.1429 | 32.14 | MATCH |  |
| SQL Pennsylvania num_loans | 207 | 207 | MATCH |  |
| SQL Pennsylvania avg_loan | 19728.9855 | 19728.99 | MATCH |  |
| SQL Pennsylvania avg_property_value | 110177.4302 | 110177.43 | MATCH |  |
| SQL Pennsylvania default_rate_pct | 18.8406 | 18.84 | MATCH |  |
| STEP5 DebtCon/Default LOAN N | 655 | 655 | MATCH |  |
| STEP5 DebtCon/Default LOAN Mean | 18293.1298 | 18293.1298 | MATCH |  |
| STEP5 DebtCon/Default LOAN Std | 10172.6006 | 10172.6006 | MATCH |  |
| STEP5 DebtCon/Default LOAN Median | 16000 | 16000.0 | MATCH |  |
| STEP5 DebtCon/Default LTV N | 655 | 655 | MATCH |  |
| STEP5 DebtCon/Default LTV Mean | 0.7009 | 0.7009 | MATCH |  |
| STEP5 DebtCon/Default LTV Std | 0.1698 | 0.1698 | MATCH |  |
| STEP5 DebtCon/Default LTV Median | 0.7241 | 0.7241 | MATCH |  |
| STEP5 DebtCon/Default DEBTINC N | 241 | 241 | MATCH |  |
| STEP5 DebtCon/Default DEBTINC Mean | 40.8511 | 40.8511 | MATCH |  |
| STEP5 DebtCon/Default DEBTINC Std | 17.2004 | 17.2004 | MATCH |  |
| STEP5 DebtCon/Default DEBTINC Median | 39.1401 | 39.1401 | MATCH |  |
| STEP5 DebtCon/Paid LOAN N | 3010 | 3010 | MATCH |  |
| STEP5 DebtCon/Paid LOAN Mean | 19964.4186 | 19964.4186 | MATCH |  |
| STEP5 DebtCon/Paid LOAN Std | 10456.5568 | 10456.5568 | MATCH |  |
| STEP5 DebtCon/Paid LOAN Median | 18050 | 18050.0 | MATCH |  |
| STEP5 DebtCon/Paid LTV N | 3010 | 3010 | MATCH |  |
| STEP5 DebtCon/Paid LTV Mean | 0.698 | 0.698 | MATCH |  |
| STEP5 DebtCon/Paid LTV Std | 0.2139 | 0.2139 | MATCH |  |
| STEP5 DebtCon/Paid LTV Median | 0.7181 | 0.7181 | MATCH |  |
| STEP5 DebtCon/Paid DEBTINC N | 2705 | 2705 | MATCH |  |
| STEP5 DebtCon/Paid DEBTINC Mean | 33.9609 | 33.9609 | MATCH |  |
| STEP5 DebtCon/Paid DEBTINC Std | 6.3842 | 6.3842 | MATCH |  |
| STEP5 DebtCon/Paid DEBTINC Median | 35.1882 | 35.1882 | MATCH |  |
| STEP5 HomeImp/Default LOAN N | 310 | 310 | MATCH |  |
| STEP5 HomeImp/Default LOAN Mean | 12839.3548 | 12839.3548 | MATCH |  |
| STEP5 HomeImp/Default LOAN Std | 11635.9642 | 11635.9642 | MATCH |  |
| STEP5 HomeImp/Default LOAN Median | 10000 | 10000.0 | MATCH |  |
| STEP5 HomeImp/Default LTV N | 310 | 310 | MATCH |  |
| STEP5 HomeImp/Default LTV Mean | 0.6688 | 0.6688 | MATCH |  |
| STEP5 HomeImp/Default LTV Std | 0.2059 | 0.2059 | MATCH |  |
| STEP5 HomeImp/Default LTV Median | 0.7107 | 0.7107 | MATCH |  |
| STEP5 HomeImp/Default DEBTINC N | 100 | 100 | MATCH |  |
| STEP5 HomeImp/Default DEBTINC Mean | 38.4888 | 38.4888 | MATCH |  |
| STEP5 HomeImp/Default DEBTINC Std | 17.3739 | 17.3739 | MATCH |  |
| STEP5 HomeImp/Default DEBTINC Median | 36.3683 | 36.3683 | MATCH |  |
| STEP5 HomeImp/Paid LOAN N | 1203 | 1203 | MATCH |  |
| STEP5 HomeImp/Paid LOAN Mean | 16848.6284 | 16848.6284 | MATCH |  |
| STEP5 HomeImp/Paid LOAN Std | 12409.8401 | 12409.8401 | MATCH |  |
| STEP5 HomeImp/Paid LOAN Median | 13600 | 13600.0 | MATCH |  |
| STEP5 HomeImp/Paid LTV N | 1203 | 1203 | MATCH |  |
| STEP5 HomeImp/Paid LTV Mean | 0.6717 | 0.6717 | MATCH |  |
| STEP5 HomeImp/Paid LTV Std | 0.2058 | 0.2058 | MATCH |  |
| STEP5 HomeImp/Paid LTV Median | 0.7256 | 0.7256 | MATCH |  |
| STEP5 HomeImp/Paid DEBTINC N | 1085 | 1085 | MATCH |  |
| STEP5 HomeImp/Paid DEBTINC Mean | 33.2241 | 33.2241 | MATCH |  |
| STEP5 HomeImp/Paid DEBTINC Std | 6.8738 | 6.8738 | MATCH |  |
| STEP5 HomeImp/Paid DEBTINC Median | 34.3862 | 34.3862 | MATCH |  |
| STEP5 missing REASON class rows excluded | no NULL REASON rows | no NULL REASON rows | MATCH |  |
