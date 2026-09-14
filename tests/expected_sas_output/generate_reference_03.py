"""
Reference implementation of sas/03_aggregation_reporting.sas used to produce
the expected-output fixtures in this directory.

No SAS runtime is available in this repository, so the fixtures are generated
by evaluating the SAS program's semantics row by row in plain Python (csv +
statistics only, no Spark, no pandas). It mirrors the SAS DATA step
observation loop and PROC rules literally:

* numeric missing (``.``) sorts below every number in comparisons
* PROC FREQ / PROC MEANS CLASS / PROC TABULATE CLASS drop missing levels
* PROC MEANS N = count of non-missing analysis values, STD = sample std
  (missing when N < 2), MEDIAN = PCTLDEF 5
* PROC SQL mean() ignores missing, HAVING after GROUP BY, OUTOBS limits rows

When real SAS output (e.g. ``ODS CSV``) becomes available, drop the exported
CSVs over the fixture files here; the parity test only depends on the column
layout documented in each ``write_*`` function.

Run from the repository root:
    python tests/expected_sas_output/generate_reference_03.py
"""

import csv
import os
import statistics
from collections import Counter, OrderedDict, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
DATA = os.path.join(ROOT, "data", "home_equity.csv")

NUMERIC = ["BAD", "LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE",
           "NINQ", "CLNO", "DEBTINC", "APPDATE"]
MISSING = None  # SAS numeric '.' / blank character value


def sas_gt(a, b):
    return a is not MISSING and a > b


def sas_lt(a, b):
    return a is MISSING or a < b


def propcase(text, delimiters=" /-(.\t"):
    if text is MISSING:
        return MISSING
    out, cap = [], True
    for ch in text.lower():
        if cap and ch.isalpha():
            out.append(ch.upper())
            cap = False
        else:
            out.append(ch)
        if ch in delimiters:
            cap = True
    return "".join(out)


def proc_import(path):
    with open(path, encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            obs = {}
            for k, v in row.items():
                if k in NUMERIC:
                    obs[k] = float(v) if v != "" else MISSING
                else:
                    obs[k] = v if v != "" else MISSING
            yield obs


def data_step_02(observations):
    """sas/02_data_cleaning.sas: home_equity -> home_equity_final."""
    for obs in observations:
        # data work.home_equity_clean;
        if obs["VALUE"] is not MISSING and obs["MORTDUE"] is not MISSING and sas_gt(obs["VALUE"], 0):
            obs["LTV"] = obs["MORTDUE"] / obs["VALUE"]
        else:
            obs["LTV"] = MISSING
        if obs["BAD"] == 0:
            obs["LOAN_OUTCOME"] = "Paid"
        elif obs["BAD"] == 1:
            obs["LOAN_OUTCOME"] = "Default"
        else:
            obs["LOAN_OUTCOME"] = MISSING
        obs["CITY"] = propcase(obs["CITY"])

        # data work.home_equity_imputed;
        for var in ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]:
            obs[f"{var}_MISS"] = 1.0 if obs[var] is MISSING else 0.0

        # data work.home_equity_filtered;
        if not (obs["LOAN"] is not MISSING and obs["VALUE"] is not MISSING and obs["BAD"] is not MISSING):
            continue

        # data work.home_equity_final;
        if not (sas_gt(obs["LTV"], 0) and sas_lt(obs["LTV"], 5)):
            continue
        if not sas_gt(obs["LOAN"], 0):
            continue
        if not sas_gt(obs["VALUE"], 0):
            continue
        yield obs


# --- statistics with PROC MEANS semantics ---------------------------------
def n(values):
    return float(len(values))


def mean(values):
    return sum(values) / len(values) if values else MISSING


def std(values):
    return statistics.stdev(values) if len(values) >= 2 else MISSING


def median_pctldef5(values):
    if not values:
        return MISSING
    xs = sorted(values)
    np_ = len(xs) * 0.5
    j = int(np_)
    g = np_ - j
    if g == 0:
        return (xs[j - 1] + xs[j]) / 2
    return xs[j]


def vmin(values):
    return min(values) if values else MISSING


def vmax(values):
    return max(values) if values else MISSING


STATS = {"N": n, "MEAN": mean, "MEDIAN": median_pctldef5, "STD": std, "MIN": vmin, "MAX": vmax}


def fmt(v):
    if v is MISSING:
        return ""
    if isinstance(v, float):
        return repr(v)
    return str(v)


def write_csv(name, header, rows):
    path = os.path.join(HERE, name)
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for r in rows:
            w.writerow([fmt(v) for v in r])
    print(f"wrote {path} ({len(rows)} rows)")


# --- Step 1: PROC FREQ ------------------------------------------------------
def write_step1(final):
    rows = []
    for var in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
        counts = Counter(o[var] for o in final if o[var] is not MISSING)
        total = sum(counts.values())
        for value in sorted(counts):
            rows.append([var, value, float(counts[value]), counts[value] * 100.0 / total])
    write_csv("03_step1_freq_tables.csv", ["VARIABLE", "VALUE", "FREQUENCY", "PERCENT"], rows)


# --- Steps 2 & 5: PROC MEANS with CLASS --------------------------------------
def proc_means(final, class_vars, analysis_vars, stats):
    groups = defaultdict(list)
    for o in final:
        if any(o[c] is MISSING for c in class_vars):
            continue
        groups[tuple(o[c] for c in class_vars)].append(o)
    rows = []
    for key in sorted(groups):
        obs = groups[key]
        for var in analysis_vars:
            values = [o[var] for o in obs if o[var] is not MISSING]
            rows.append(list(key) + [float(len(obs)), var] + [STATS[s](values) for s in stats])
    return rows


def write_step2(final):
    stats = ["N", "MEAN", "MEDIAN", "STD", "MIN", "MAX"]
    rows = proc_means(final, ["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "DEBTINC"], stats)
    write_csv("03_step2_means_by_outcome.csv", ["LOAN_OUTCOME", "N_OBS", "VARIABLE"] + stats, rows)


def write_step5(final):
    stats = ["N", "MEAN", "STD", "MEDIAN"]
    rows = proc_means(final, ["REASON", "LOAN_OUTCOME"], ["LOAN", "LTV", "DEBTINC"], stats)
    write_csv("03_step5_means_by_reason_outcome.csv",
              ["REASON", "LOAN_OUTCOME", "N_OBS", "VARIABLE"] + stats, rows)


# --- Step 3: PROC TABULATE ----------------------------------------------------
def write_step3(final):
    kept = [o for o in final if o["JOB"] is not MISSING and o["REGION"] is not MISSING]
    cells = defaultdict(list)
    for o in kept:
        for job in (o["JOB"], "Total"):
            for region in (o["REGION"], "Total"):
                cells[(job, region)].append(o["BAD"])
    jobs = sorted({o["JOB"] for o in kept}) + ["Total"]
    regions = sorted({o["REGION"] for o in kept}) + ["Total"]
    rows = []
    for job in jobs:
        for region in regions:
            bad = [b for b in cells.get((job, region), []) if b is not MISSING]
            if bad:
                rows.append([job, region, float(len(bad)), mean(bad)])
    write_csv("03_step3_tabulate_default_rates.csv", ["JOB", "REGION", "N", "MEAN_BAD"], rows)


# --- Step 4: PROC SQL -----------------------------------------------------------
def write_step4(final):
    groups = defaultdict(list)
    for o in final:
        groups[o["STATE"]].append(o)
    rows = []
    for state, obs in groups.items():
        if len(obs) >= 10:  # having count(*) >= 10
            rows.append([
                state,
                float(len(obs)),
                mean([o["LOAN"] for o in obs if o["LOAN"] is not MISSING]),
                mean([o["VALUE"] for o in obs if o["VALUE"] is not MISSING]),
                mean([o["BAD"] for o in obs if o["BAD"] is not MISSING]),
            ])
    rows.sort(key=lambda r: (-r[2], r[0]))  # order by avg_loan desc (STATE breaks ties)
    rows = rows[:10]  # outobs=10
    write_csv("03_step4_sql_top_states.csv",
              ["STATE", "num_loans", "avg_loan", "avg_property_value", "default_rate"], rows)


def main():
    final = list(data_step_02(proc_import(DATA)))
    print(f"work.home_equity_final: {len(final)} observations")
    with open(os.path.join(HERE, "03_home_equity_final_nobs.txt"), "w") as fh:
        fh.write(f"{len(final)}\n")
    write_step1(final)
    write_step2(final)
    write_step3(final)
    write_step4(final)
    write_step5(final)


if __name__ == "__main__":
    main()
