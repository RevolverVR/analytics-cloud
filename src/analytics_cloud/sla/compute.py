from __future__ import annotations

import os
import duckdb
import sys
from pathlib import Path

REPORT_PATH = os.getenv("ALERT_REPORT_PATH", "data/alert_report.txt")

def _write(line: str) -> None:
    # stdout + file
    print(line)
    Path(REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")



DB_PATH = os.getenv("DUCKDB_PATH", "data/dev.duckdb")


def main() -> None:
    # reset report file
    Path(REPORT_PATH).write_text('', encoding='utf-8')
    con = duckdb.connect(DB_PATH)

    summary = con.execute(
        """
        select
          count(*) as tickets,
          round(avg(first_response_minutes), 2) as avg_fr_min,
          round(100.0 * avg(case when sla_breach then 1 else 0 end), 2) as breach_pct,
          round(100.0 * avg(case when fcr_proxy then 1 else 0 end), 2) as fcr_pct
        from main.mart_ticket_metrics
        """
    ).fetchdf()

    last7 = con.execute(
        """
        with bounds as (
          select max(created_ts) as max_created
          from main.mart_ticket_metrics
        )
        select
          count(*) as tickets_last7,
          round(100.0 * avg(case when sla_breach then 1 else 0 end), 2) as breach_pct_last7,
          (select max_created from bounds) as as_of_created_ts
        from main.mart_ticket_metrics, bounds
        where created_ts >= (select max_created from bounds) - interval 7 day
        """
    ).fetchdf()

    top = con.execute(
        """
        select
          request_id,
          priority,
          request_status,
          first_response_minutes,
          created_ts,
          first_responded_ts
        from main.mart_ticket_metrics
        where sla_breach = true
        order by first_response_minutes desc nulls last
        limit 10
        """
    ).fetchdf()

    _write("=== ALERT REPORT (local) ===")
    _write(f"DB: {DB_PATH}")
    _write("\n-- overall --")
    _write(summary.to_string(index=False))
    _write("\n-- last 7 days --")
    _write(last7.to_string(index=False))
    _write("\n-- top 10 breaches --")
    if len(top) == 0:
        _write("(none)")
    else:
        _write(top.to_string(index=False))


if __name__ == "__main__":
    main()
