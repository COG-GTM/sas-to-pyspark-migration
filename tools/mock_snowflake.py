"""Mock Snowflake data source for the migration enrichment demo.

Stands in for a real Snowflake connection (``snowflake.connector``). It exposes a
deliberately small subset of the DB-API surface (``connect`` ->
``cursor`` -> ``execute`` / ``fetchall``) so that ``enrich_context.py`` reads the
same way it would against a live warehouse. Under the hood it queries the bundled
``data/home_equity.csv`` with DuckDB.

The point of the demo is *data-profile parity*: before migrating a SAS module,
Devin profiles the source/target tables (row counts, default-rate-by-segment)
so the PySpark port can be validated against a known baseline.

Swap this module for the real thing by setting SNOWFLAKE_* env vars and using
``snowflake.connector.connect`` with the same SQL.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_CSV = REPO_ROOT / "data" / "home_equity.csv"

# Logical -> physical mapping. In a real warehouse these are Snowflake tables;
# here every logical table resolves to the bundled CSV.
_TABLE = f"read_csv_auto('{DATA_CSV}', header=true)"

# RISK_SEGMENT derivation mirrors sas/04_risk_segmentation.sas (the SAS baseline).
_RISK_SCORE_SQL = """
(
  CASE WHEN VALUE IS NOT NULL AND VALUE <> 0 AND (MORTDUE / VALUE) >= 0.80 THEN 3.0
       WHEN VALUE IS NOT NULL AND VALUE <> 0 AND (MORTDUE / VALUE) >= 0.60 THEN 1.5
       ELSE 0 END
  + CASE WHEN DEBTINC >= 50 THEN 3.0 WHEN DEBTINC >= 40 THEN 2.0 WHEN DEBTINC >= 30 THEN 1.0 ELSE 0 END
  + CASE WHEN DELINQ >= 4 THEN 2.0 WHEN DELINQ >= 2 THEN 1.5 WHEN DELINQ = 1 THEN 0.5 ELSE 0 END
  + CASE WHEN DEROG >= 3 THEN 2.0 WHEN DEROG >= 1 THEN 1.0 ELSE 0 END
)
"""

_RISK_SEGMENT_SQL = f"""
CASE WHEN {_RISK_SCORE_SQL} < 3 THEN 'Low Risk'
     WHEN {_RISK_SCORE_SQL} < 5 THEN 'Medium Risk'
     WHEN {_RISK_SCORE_SQL} < 7 THEN 'High Risk'
     ELSE 'Very High Risk' END
"""

# Canned analytical queries the enrichment step runs against the "warehouse".
QUERIES = {
    "row_count": f"SELECT COUNT(*) AS n_rows FROM {_TABLE}",
    "default_rate_overall": (
        f"SELECT COUNT(*) AS n, AVG(CAST(BAD AS DOUBLE)) AS default_rate FROM {_TABLE}"
    ),
    "default_rate_by_segment": f"""
        SELECT {_RISK_SEGMENT_SQL} AS risk_segment,
               COUNT(*) AS n_loans,
               ROUND(AVG(CAST(BAD AS DOUBLE)), 4) AS default_rate
        FROM {_TABLE}
        GROUP BY risk_segment
        ORDER BY default_rate
    """,
}


class _Cursor:
    def __init__(self, conn: "MockSnowflakeConnection") -> None:
        self._conn = conn
        self._result: list[tuple[Any, ...]] = []
        self.description: list[tuple[str, ...]] = []

    def execute(self, sql: str) -> "_Cursor":
        rel = self._conn._db.sql(sql)
        self.description = [(c,) for c in rel.columns]
        self._result = [tuple(row) for row in rel.fetchall()]
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._result

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._result[0] if self._result else None

    @property
    def columns(self) -> list[str]:
        return [d[0] for d in self.description]

    def close(self) -> None:  # pragma: no cover - parity with DB-API
        pass


class MockSnowflakeConnection:
    """DB-API-ish handle backed by DuckDB over the bundled CSV."""

    def __init__(self, **kwargs: Any) -> None:
        self.account = kwargs.get("account", "MOCK-ACCOUNT")
        self.database = kwargs.get("database", "ANALYTICS")
        self.schema = kwargs.get("schema", "RISK")
        self._db = duckdb.connect(database=":memory:")

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def close(self) -> None:  # pragma: no cover
        self._db.close()


def connect(**kwargs: Any) -> MockSnowflakeConnection:
    """Mirror of ``snowflake.connector.connect``.

    If ``SNOWFLAKE_ACCOUNT`` is set, this is where a real connection would be
    opened instead; the demo always returns the mock.
    """
    if os.getenv("SNOWFLAKE_ACCOUNT"):
        # Placeholder for the live path; kept mock for the demo.
        pass
    return MockSnowflakeConnection(**kwargs)


def profile_source() -> dict[str, Any]:
    """Run the canned profile queries and return a structured baseline."""
    conn = connect(database="ANALYTICS", schema="RISK")
    cur = conn.cursor()

    n_rows = cur.execute(QUERIES["row_count"]).fetchone()[0]

    cur.execute(QUERIES["default_rate_overall"])
    overall = cur.fetchone()

    cur.execute(QUERIES["default_rate_by_segment"])
    seg_cols = cur.columns
    segments = [dict(zip(seg_cols, row)) for row in cur.fetchall()]

    conn.close()
    return {
        "source": "ANALYTICS.RISK.HOME_EQUITY (mock Snowflake)",
        "row_count": int(n_rows),
        "overall_default_rate": round(float(overall[1]), 4),
        "default_rate_by_segment": segments,
    }


if __name__ == "__main__":
    import json

    print(json.dumps(profile_source(), indent=2))
