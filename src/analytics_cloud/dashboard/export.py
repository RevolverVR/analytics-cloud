from __future__ import annotations

import os
from pathlib import Path
import duckdb

DB_PATH = os.getenv("DUCKDB_PATH", "data/dev.duckdb")
OUT_DIR = Path(os.getenv("DASH_EXPORT_DIR", "data/exports"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main() -> None:
    con = duckdb.connect(DB_PATH)

    # CSV
    csv_path = OUT_DIR / "mart_ticket_metrics.csv"
    con.execute(f"""
        copy (
            select *
            from main.mart_ticket_metrics
        ) to '{csv_path.as_posix()}'
        (header, delimiter ',')
    """)

    # Parquet (mejor para BI/dashboards y futuro cloud)
    pq_path = OUT_DIR / "mart_ticket_metrics.parquet"
    con.execute(f"""
        copy (
            select *
            from main.mart_ticket_metrics
        ) to '{pq_path.as_posix()}'
        (format parquet)
    """)

    print(f"[export] wrote: {csv_path} ({csv_path.stat().st_size} bytes)")
    print(f"[export] wrote: {pq_path} ({pq_path.stat().st_size} bytes)")

if __name__ == "__main__":
    main()
