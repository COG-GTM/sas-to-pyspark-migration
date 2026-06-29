# Migration context enrichment

`enrich_context.py` is the **pre-work enrichment** step Devin runs *before*
writing migration code for a SAS module. It gathers the operational context a
human engineer would otherwise chase down manually and posts a consolidated
report onto the Jira task.

| Source | Mode | What it answers |
|---|---|---|
| GitHub Actions | **live** | Is the current build/branch green before I start? |
| SonarCloud | **live** | What's the code-quality gate + baseline measures? |
| Snowflake (`mock_snowflake.py`) | **mock** | Source row counts + default-rate-by-segment to validate parity against |
| CMDB (`cmdb/cmdb_data.json`) | **mock** | Which downstream systems consume this job? Owners, SLAs, approvers, blast radius |

## Usage

```bash
pip install -r ../requirements-dev.txt

# Print the enrichment report for module 04
python enrich_context.py --module 04_risk_segmentation --branch main

# Also post it as a comment on a Jira task
python enrich_context.py --module 04_risk_segmentation --post-to LEIG-52
```

## Configuration (env vars)

- `GH_TOKEN` / `GITHUB_TOKEN` — GitHub API (falls back to `gh auth token` locally)
- `SONAR_TOKEN` — SonarCloud API
- `JIRA_EMAIL`, `JIRA_API_TOKEN` — required only for `--post-to`
- Optional overrides: `GITHUB_REPO`, `SONAR_ORG`, `SONAR_PROJECT`, `JIRA_BASE_URL`

## Going live on the mocked sources

- **Snowflake** — set `SNOWFLAKE_*` env vars and replace `mock_snowflake.connect`
  with `snowflake.connector.connect`. The SQL in `QUERIES` is warehouse-ready.
- **CMDB** — swap `fetch_cmdb_impact` to call the ServiceNow Table API
  (`/api/now/table/cmdb_ci`) instead of reading the bundled JSON.
