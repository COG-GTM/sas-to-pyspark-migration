#!/usr/bin/env python3
"""Pre-work context enrichment for SAS->PySpark migration tasks.

Before Devin writes a line of migration code on a task, it runs this step to
gather the operational context that a human engineer would normally chase down
by hand:

  1. CI status          (LIVE)  - latest GitHub Actions run for the repo/branch
  2. Code quality gate  (LIVE)  - SonarCloud quality gate + key measures
  3. Source data profile(MOCK)  - Snowflake row counts + default-rate-by-segment
  4. Downstream impact  (MOCK)  - CMDB consumers, owners, approvers, change window

It prints a consolidated Markdown report and, with --post-to LEIG-123, posts it
as a comment on the Jira task so the enrichment is captured on the board.

Env vars used:
  GH_TOKEN / GITHUB_TOKEN  - GitHub API (CI status)
  SONAR_TOKEN              - SonarCloud API (quality gate)
  JIRA_EMAIL, JIRA_API_TOKEN - Jira REST (optional, for --post-to)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mock_snowflake  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
CMDB_FILE = REPO_ROOT / "tools" / "cmdb" / "cmdb_data.json"

GITHUB_REPO = os.getenv("GITHUB_REPO", "COG-GTM/sas-to-pyspark-migration")
SONAR_ORG = os.getenv("SONAR_ORG", "cogleighton-edd")
SONAR_PROJECT = os.getenv("SONAR_PROJECT", "cogleighton-edd_sas-to-pyspark-migration")
JIRA_BASE = os.getenv("JIRA_BASE_URL", "https://cog-gtm.atlassian.net")

TIMEOUT = 20


# --------------------------------------------------------------------------- #
# 1. CI status (LIVE - GitHub Actions)
# --------------------------------------------------------------------------- #
def _github_token() -> str | None:
    token = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    if token:
        return token
    # Fall back to the gh CLI's stored credential for local/demo runs.
    try:
        import subprocess

        out = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, timeout=10
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except (OSError, ValueError):
        pass
    return None


def fetch_ci_status(branch: str | None) -> dict:
    token = _github_token()
    if not token:
        return {"available": False, "reason": "no GitHub token (GH_TOKEN / gh auth)"}
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs"
    params = {"per_page": 1}
    if branch:
        params["branch"] = branch
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        runs = resp.json().get("workflow_runs", [])
    except requests.RequestException as exc:
        return {"available": False, "reason": str(exc)}
    if not runs:
        return {"available": True, "found": False, "branch": branch}
    run = runs[0]
    return {
        "available": True,
        "found": True,
        "workflow": run.get("name"),
        "branch": run.get("head_branch"),
        "status": run.get("status"),
        "conclusion": run.get("conclusion"),
        "commit": (run.get("head_sha") or "")[:8],
        "url": run.get("html_url"),
    }


# --------------------------------------------------------------------------- #
# 2. Code quality gate (LIVE - SonarCloud)
# --------------------------------------------------------------------------- #
def fetch_sonar_status() -> dict:
    token = os.getenv("SONAR_TOKEN")
    if not token:
        return {"available": False, "reason": "SONAR_TOKEN not set"}
    auth = (token, "")
    try:
        qg = requests.get(
            "https://sonarcloud.io/api/qualitygates/project_status",
            params={"projectKey": SONAR_PROJECT},
            auth=auth,
            timeout=TIMEOUT,
        )
        qg.raise_for_status()
        status = qg.json().get("projectStatus", {})

        metric_keys = "bugs,vulnerabilities,code_smells,coverage,duplicated_lines_density,ncloc"
        meas = requests.get(
            "https://sonarcloud.io/api/measures/component",
            params={"component": SONAR_PROJECT, "metricKeys": metric_keys},
            auth=auth,
            timeout=TIMEOUT,
        )
        meas.raise_for_status()
        measures = {
            m["metric"]: m.get("value")
            for m in meas.json().get("component", {}).get("measures", [])
        }
    except requests.RequestException as exc:
        return {"available": False, "reason": str(exc)}
    return {
        "available": True,
        "gate_status": status.get("status", "NONE"),
        "conditions": [
            {
                "metric": c.get("metricKey"),
                "status": c.get("status"),
                "actual": c.get("actualValue"),
            }
            for c in status.get("conditions", [])
        ],
        "measures": measures,
        "url": f"https://sonarcloud.io/project/overview?id={SONAR_PROJECT}",
    }


# --------------------------------------------------------------------------- #
# 3. Source data profile (MOCK - Snowflake)
# --------------------------------------------------------------------------- #
def fetch_data_profile() -> dict:
    try:
        return {"available": True, **mock_snowflake.profile_source()}
    except Exception as exc:  # noqa: BLE001 - report any profiling failure
        return {"available": False, "reason": str(exc)}


# --------------------------------------------------------------------------- #
# 4. Downstream impact (MOCK - CMDB)
# --------------------------------------------------------------------------- #
def fetch_cmdb_impact(module: str) -> dict:
    try:
        cmdb = json.loads(CMDB_FILE.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return {"available": False, "reason": str(exc)}
    ci_id = cmdb.get("job_to_ci", {}).get(module)
    if not ci_id:
        return {"available": True, "found": False, "module": module}
    ci = cmdb["configuration_items"].get(ci_id, {})
    return {
        "available": True,
        "found": True,
        "ci_id": ci_id,
        "ci_name": ci.get("name"),
        "business_service": ci.get("business_service"),
        "criticality": ci.get("criticality"),
        "owner_team": ci.get("owner_team"),
        "change_window": ci.get("change_window"),
        "downstream_consumers": ci.get("downstream_consumers", []),
        "approvers": ci.get("approvers", []),
    }


# --------------------------------------------------------------------------- #
# Report rendering
# --------------------------------------------------------------------------- #
def _icon(ok: bool) -> str:
    return "[OK]" if ok else "[!!]"


def render_report(module: str, ci: dict, sonar: dict, data: dict, cmdb: dict) -> str:
    lines: list[str] = []
    lines.append(f"## Context enrichment — `{module}`")
    lines.append("")
    lines.append("_Automated pre-work context gathered before migration coding begins._")
    lines.append("")

    # CI
    lines.append("### 1. CI status (live — GitHub Actions)")
    if not ci.get("available"):
        lines.append(f"- Unavailable: {ci.get('reason')}")
    elif not ci.get("found"):
        lines.append(f"- No workflow runs found yet for branch `{ci.get('branch')}`.")
    else:
        ok = ci.get("conclusion") in (None, "success")
        lines.append(f"- {_icon(ok)} **{ci.get('conclusion') or ci.get('status')}** "
                     f"— workflow `{ci.get('workflow')}` on `{ci.get('branch')}` "
                     f"(`{ci.get('commit')}`)")
        if ci.get("url"):
            lines.append(f"- Run: {ci['url']}")
    lines.append("")

    # Sonar
    lines.append("### 2. Code quality gate (live — SonarCloud)")
    if not sonar.get("available"):
        lines.append(f"- Unavailable: {sonar.get('reason')}")
    else:
        gate = sonar.get("gate_status", "NONE")
        ok = gate in ("OK", "NONE")
        lines.append(f"- {_icon(ok)} Quality gate: **{gate}**")
        m = sonar.get("measures", {})
        if m:
            lines.append(
                f"- Measures: bugs={m.get('bugs','-')}, "
                f"vulnerabilities={m.get('vulnerabilities','-')}, "
                f"code_smells={m.get('code_smells','-')}, "
                f"coverage={m.get('coverage','-')}%, "
                f"duplication={m.get('duplicated_lines_density','-')}%, "
                f"lines={m.get('ncloc','-')}"
            )
        if sonar.get("url"):
            lines.append(f"- Project: {sonar['url']}")
    lines.append("")

    # Data profile
    lines.append("### 3. Source data profile (mock — Snowflake)")
    if not data.get("available"):
        lines.append(f"- Unavailable: {data.get('reason')}")
    else:
        lines.append(f"- Source: `{data.get('source')}`")
        lines.append(f"- Row count baseline: **{data.get('row_count'):,}**")
        lines.append(f"- Overall default rate: **{data.get('overall_default_rate')}**")
        lines.append("- Default rate by risk segment (SAS baseline to match):")
        lines.append("")
        lines.append("  | Risk segment | Loans | Default rate |")
        lines.append("  |---|---:|---:|")
        for seg in data.get("default_rate_by_segment", []):
            lines.append(
                f"  | {seg.get('risk_segment')} | {int(seg.get('n_loans')):,} "
                f"| {seg.get('default_rate')} |"
            )
    lines.append("")

    # CMDB
    lines.append("### 4. Downstream impact (mock — CMDB)")
    if not cmdb.get("available"):
        lines.append(f"- Unavailable: {cmdb.get('reason')}")
    elif not cmdb.get("found"):
        lines.append(f"- No CI mapped for module `{module}`.")
    else:
        lines.append(f"- CI: **{cmdb.get('ci_name')}** (`{cmdb.get('ci_id')}`)")
        lines.append(f"- Business service: {cmdb.get('business_service')}")
        lines.append(f"- Criticality: **{cmdb.get('criticality')}** · "
                     f"Owner: {cmdb.get('owner_team')} · "
                     f"Change window: {cmdb.get('change_window')}")
        consumers = cmdb.get("downstream_consumers", [])
        if consumers:
            lines.append(f"- **{len(consumers)} downstream consumer(s)** — blast radius:")
            for c in consumers:
                lines.append(f"  - **{c.get('name')}** ({c.get('type')}, "
                             f"owner {c.get('owner_team')}, SLA {c.get('sla')}): "
                             f"{c.get('impact_if_broken')}")
        approvers = cmdb.get("approvers", [])
        if approvers:
            who = ", ".join(f"{a.get('name')} ({a.get('role')})" for a in approvers)
            lines.append(f"- Required approvers: {who}")
    lines.append("")

    lines.append("---")
    lines.append("_Sources: GitHub Actions + SonarCloud (live); Snowflake + CMDB (mocked for demo)._")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Jira posting
# --------------------------------------------------------------------------- #
def post_to_jira(issue_key: str, body: str) -> None:
    email = os.getenv("JIRA_EMAIL")
    token = os.getenv("JIRA_API_TOKEN")
    if not (email and token):
        print("[enrich] JIRA_EMAIL/JIRA_API_TOKEN not set; skipping Jira post.", file=sys.stderr)
        return
    url = f"{JIRA_BASE}/rest/api/2/issue/{issue_key}/comment"
    resp = requests.post(url, auth=(email, token), json={"body": body}, timeout=TIMEOUT)
    if resp.status_code >= 300:
        print(f"[enrich] Jira post failed ({resp.status_code}): {resp.text[:300]}", file=sys.stderr)
    else:
        print(f"[enrich] Posted enrichment report to {issue_key}.", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module", default="04_risk_segmentation",
                        help="Migration module key (e.g. 04_risk_segmentation)")
    parser.add_argument("--branch", default=None, help="Branch for CI lookup")
    parser.add_argument("--post-to", default=None, help="Jira issue key to comment on")
    parser.add_argument("--json", action="store_true", help="Also emit raw JSON to stderr")
    args = parser.parse_args()

    ci = fetch_ci_status(args.branch)
    sonar = fetch_sonar_status()
    data = fetch_data_profile()
    cmdb = fetch_cmdb_impact(args.module)

    report = render_report(args.module, ci, sonar, data, cmdb)
    print(report)

    if args.json:
        print(json.dumps({"ci": ci, "sonar": sonar, "data": data, "cmdb": cmdb},
                         indent=2, default=str), file=sys.stderr)

    if args.post_to:
        post_to_jira(args.post_to, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
