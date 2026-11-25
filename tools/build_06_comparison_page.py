"""
Build docs/06_ltv_bands_comparison.html: input data vs SAS-expected vs PySpark output
for the sas/06_simple_ltv_bands.sas migration. Runs the PySpark script logic for real.

Usage: python tools/build_06_comparison_page.py
"""

import csv
import html
import importlib.util
import os
import sys
from collections import defaultdict
from itertools import zip_longest
from datetime import datetime, timezone

from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "tests"))
from sas_reference import DATA_PATH, EXPECTED_PATH, ltv_band_rows, row_key  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "simple_ltv_bands", os.path.join(PROJECT_ROOT, "pyspark", "06_simple_ltv_bands.py"))
simple_ltv_bands = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(simple_ltv_bands)

OUT_PATH = os.path.join(PROJECT_ROOT, "docs", "06_ltv_bands_comparison.html")
SAMPLE_ROWS = 12
TOL = 1e-9


def fmt(v, kind="num"):
    if v is None or v == "":
        return '<span class="miss">.</span>'
    if kind == "pct":
        return f"{float(v):.4%}"
    if kind == "ratio":
        return f"{float(v):.4f}"
    if kind == "money":
        return f"{float(v):,.2f}"
    if isinstance(v, float) and v.is_integer():
        return f"{int(v):,}"
    return html.escape(str(v))


def table(headers, rows, cls=""):
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<table class="{cls}"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'


def code_block(path, start_marker, end_line, after=None):
    """Lines from the first containing start_marker (past the line containing `after`, if given)
    through the next line equal to end_line (ignoring trailing spaces)."""
    lines = open(path).read().splitlines()
    a = next(i for i, l in enumerate(lines) if after in l) if after else -1
    s = next(i for i, l in enumerate(lines) if i > a and start_marker in l)
    e = next(i for i, l in enumerate(lines) if i > s and l.rstrip() == end_line)
    return html.escape("\n".join(lines[s:e + 1]))


def main():
    spark = SparkSession.builder.appName("Build06Comparison").master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    input_count = df.count()
    bands_df = simple_ltv_bands.build_ltv_bands(df)
    spark_bands = sorted((r.asDict() for r in bands_df.collect()), key=row_key)
    spark_summary = [r.asDict() for r in simple_ltv_bands.summarize_ltv_bands(bands_df).collect()]
    spark_version = spark.version
    spark.stop()

    with open(DATA_PATH, newline="", encoding="utf-8-sig") as f:
        raw = list(csv.DictReader(f))
    ref_bands = sorted(ltv_band_rows(), key=row_key)
    with open(EXPECTED_PATH, newline="") as f:
        expected = list(csv.DictReader(f))

    # --- input sample (raw CSV rows) -------------------------------------------------
    def filter_result(r):
        if r["LOAN"] == "" or r["VALUE"] == "":
            return '<span class="bad">dropped (LOAN/VALUE missing)</span>'
        if float(r["VALUE"]) <= 0:
            return '<span class="bad">dropped (VALUE &le; 0)</span>'
        return '<span class="ok">kept</span>'

    input_rows = [[i + 1, fmt(r["BAD"]), fmt(r["LOAN"]), fmt(r["MORTDUE"]), fmt(r["VALUE"]),
                   fmt(r["REASON"]), filter_result(r)] for i, r in enumerate(raw[:SAMPLE_ROWS])]

    # --- row-level comparison --------------------------------------------------------
    def same_row(s, e):
        if s is None or e is None:
            return False
        if any(s[c] != e[c] for c in ("BAD", "LOAN", "MORTDUE", "VALUE", "REASON", "LTV_BAND")):
            return False
        if s["LTV"] is None or e["LTV"] is None:
            return s["LTV"] is None and e["LTV"] is None
        return abs(s["LTV"] - e["LTV"]) <= 1e-12

    # Pair rows by identity key so one missing or extra row cannot shift every later pair.
    def by_key(rows):
        groups = defaultdict(list)
        for r in rows:
            groups[row_key(r)].append(r)
        return groups

    spk_groups, ref_groups = by_key(spark_bands), by_key(ref_bands)
    pairs = [pair for k in sorted(spk_groups.keys() | ref_groups.keys())
             for pair in zip_longest(spk_groups.get(k, []), ref_groups.get(k, []))]
    row_matches = sum(same_row(s, e) for s, e in pairs)
    row_total = len(pairs)
    row_mismatches = row_total - row_matches
    absent = '<span class="miss">absent</span>'
    row_cmp = []
    for s, e in pairs[:SAMPLE_ROWS]:
        ident = s or e
        row_cmp.append([fmt(ident["LOAN"]), fmt(ident["MORTDUE"]), fmt(ident["VALUE"]),
                        *([fmt(e["LTV"], "ratio"), f'<b>{html.escape(e["LTV_BAND"])}</b>'] if e else [absent] * 2),
                        *([fmt(s["LTV"], "ratio"), f'<b>{html.escape(s["LTV_BAND"])}</b>'] if s else [absent] * 2),
                        '<span class="ok">MATCH</span>' if same_row(s, e) else '<span class="bad">DIFF</span>'])

    # --- summary comparison ----------------------------------------------------------
    exp_by_band = {r["LTV_BAND"]: r for r in expected}
    spk_by_band = {r["LTV_BAND"]: r for r in spark_summary}
    sum_rows, sum_matches = [], 0
    sum_total = len(exp_by_band.keys() | spk_by_band.keys())
    for band in sorted(exp_by_band.keys() | spk_by_band.keys()):
        e, s = exp_by_band.get(band), spk_by_band.get(band)
        ok, diff = False, "n/a"
        if e and s:
            d = max(abs(float(e["AVG_LOAN"]) - s["AVG_LOAN"]), abs(float(e["DEFAULT_RATE"]) - s["DEFAULT_RATE"]))
            ok, diff = int(e["N_LOANS"]) == s["N_LOANS"] and d <= TOL, f"{d:.1e}"
        sum_matches += ok
        exp_cells = [fmt(float(e["N_LOANS"])), fmt(e["AVG_LOAN"], "money"), fmt(e["DEFAULT_RATE"], "pct")] if e else [absent] * 3
        spk_cells = [fmt(float(s["N_LOANS"])), fmt(s["AVG_LOAN"], "money"), fmt(s["DEFAULT_RATE"], "pct")] if s else [absent] * 3
        sum_rows.append([f"<b>{html.escape(band)}</b>", *exp_cells, *spk_cells, diff,
                         '<span class="ok">MATCH</span>' if ok else '<span class="bad">DIFF</span>'])

    verdict_ok = row_mismatches == 0 and sum_matches == sum_total
    verdict = (f'<div class="verdict {"pass" if verdict_ok else "fail"}">'
               f'{"PARITY PASS" if verdict_ok else "PARITY FAIL"}: {row_matches:,} of {row_total:,} rows match, '
               f'{row_mismatches} row mismatches; {sum_matches} of {sum_total} summary rows match '
               f'(tolerance {TOL:g}).</div>')

    sas_data = code_block(os.path.join(PROJECT_ROOT, "sas", "06_simple_ltv_bands.sas"), "data work.ltv_bands", "run;")
    sas_means = code_block(os.path.join(PROJECT_ROOT, "sas", "06_simple_ltv_bands.sas"), "proc means", "run;")
    py_bands = code_block(simple_ltv_bands.__file__, "ltv = when(", "    )")
    py_sum = code_block(simple_ltv_bands.__file__, "return (", "    )", after="def summarize_ltv_bands")

    page = f"""<!doctype html><html><head><meta charset="utf-8">
<title>SAS to PySpark: LTV band example</title>
<style>
body{{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:0;background:#f5f6f8;color:#1d2330}}
header{{background:#1d2a44;color:#fff;padding:22px 40px}} header h1{{margin:0;font-size:26px}}
header p{{margin:6px 0 0;color:#c5cde0}} main{{padding:20px 40px 60px;max-width:1400px}}
section{{background:#fff;border-radius:10px;padding:18px 24px;margin:18px 0;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
h2{{margin:0 0 6px;font-size:20px}} .sub{{color:#5b6478;margin:0 0 12px;font-size:14px}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:18px}} .grid>div{{min-width:0}}
pre{{background:#0f172a;color:#e2e8f0;padding:12px;border-radius:8px;font-size:12.5px;overflow:auto;margin:0;white-space:pre-wrap}}
.lbl{{font-weight:600;font-size:13px;margin:0 0 6px;text-transform:uppercase;letter-spacing:.04em}}
.sas{{color:#1f6fd1}} .spk{{color:#e25a1c}}
table{{border-collapse:collapse;width:100%;font-size:14px}} th,td{{padding:6px 10px;border-bottom:1px solid #e6e8ee;text-align:right}}
th{{background:#eef1f6;font-weight:600}} td:first-child,th:first-child{{text-align:left}}
.miss{{color:#b42318;font-weight:700}} .ok{{color:#067647;font-weight:700}} .bad{{color:#b42318;font-weight:700}}
.verdict{{font-size:18px;font-weight:700;padding:14px 18px;border-radius:8px;margin-top:6px}}
.pass{{background:#dcfae6;color:#067647}} .fail{{background:#fee4e2;color:#b42318}}
.kpis{{display:flex;gap:16px;margin-top:8px}} .kpi{{background:#eef1f6;border-radius:8px;padding:10px 16px}}
.kpi b{{display:block;font-size:22px}} .note{{font-size:13px;color:#5b6478}}
th.sasg{{background:#dbe8fb}} th.spkg{{background:#fde4d8}}
</style></head><body>
<header><h1>SAS &rarr; PySpark: simple LTV band migration</h1>
<p>sas/06_simple_ltv_bands.sas &rarr; pyspark/06_simple_ltv_bands.py &middot; data/home_equity.csv &middot; Spark {spark_version} &middot; generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}</p></header>
<main>
<section><h2>1. Input data</h2><p class="sub">First {SAMPLE_ROWS} raw rows of data/home_equity.csv (5 of 18 columns used). <span class="miss">.</span> = missing value.</p>
<div class="kpis"><div class="kpi">Input rows<b>{input_count:,}</b></div><div class="kpi">Rows after filter (LOAN, VALUE present, VALUE &gt; 0)<b>{len(spark_bands):,}</b></div><div class="kpi">Rows dropped<b>{input_count - len(spark_bands):,}</b></div></div><br>
{table(["#", "BAD", "LOAN", "MORTDUE", "VALUE", "REASON", "Subsetting IF"], input_rows)}</section>

<section><h2>2. The migration</h2><p class="sub">DATA step &rarr; DataFrame transformations; PROC MEANS NWAY CLASS &rarr; groupBy().agg().</p>
<div class="grid"><div><p class="lbl sas">SAS</p><pre>{sas_data}\n\n{sas_means}</pre></div>
<div><p class="lbl spk">PySpark</p><pre>{py_bands}\n\n{py_sum}</pre></div></div></section>

<section><h2>3. Row-level output: SAS-expected vs PySpark</h2><p class="sub">First {SAMPLE_ROWS} filtered rows in BAD, LOAN, MORTDUE, VALUE, REASON order (rows are paired on these columns because Spark guarantees no row order); all {row_total:,} pairs are compared, {row_mismatches} mismatches.</p>
<table><thead><tr><th>LOAN</th><th>MORTDUE</th><th>VALUE</th><th class="sasg">SAS LTV</th><th class="sasg">SAS LTV_BAND</th><th class="spkg">PySpark LTV</th><th class="spkg">PySpark LTV_BAND</th><th>Check</th></tr></thead>
<tbody>{"".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in row_cmp)}</tbody></table></section>

<section><h2>4. Summary output: SAS-expected vs PySpark</h2><p class="sub">work.ltv_band_summary (expected/06_ltv_band_summary_sas_expected.csv) vs PySpark result.</p>
<table><thead><tr><th>LTV_BAND</th><th class="sasg">SAS N_LOANS</th><th class="sasg">SAS AVG_LOAN</th><th class="sasg">SAS DEFAULT_RATE</th><th class="spkg">PySpark N_LOANS</th><th class="spkg">PySpark AVG_LOAN</th><th class="spkg">PySpark DEFAULT_RATE</th><th>Max abs diff</th><th>Check</th></tr></thead>
<tbody>{"".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in sum_rows)}</tbody></table>
{verdict}
<p class="note">How the SAS-expected output was derived: no SAS runtime was available, so tests/sas_reference.py re-implements the SAS program in plain Python (csv module, no Spark) using SAS semantics: empty cells are missing (.), missing sorts below every number, N()/MEAN() skip missing, output sorted BY LTV_BAND. The PySpark column comes from running pyspark/06_simple_ltv_bands.py logic on local Spark.</p></section>
</main></body></html>"""
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        f.write(page)
    print(verdict.split(">", 1)[1].rsplit("<", 1)[0])
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
