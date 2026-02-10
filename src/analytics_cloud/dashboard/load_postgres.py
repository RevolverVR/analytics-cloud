from __future__ import annotations

import os
from pathlib import Path
import duckdb
import pandas as pd
import psycopg

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/dev.duckdb")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "analytics")
PG_USER = os.getenv("PG_USER", "analytics")
PG_PASSWORD = os.getenv("PG_PASSWORD", "analytics")

TABLE = os.getenv("PG_TABLE", "mart_ticket_metrics")
TMP = Path("data/exports/_tmp_mart_ticket_metrics.csv")


def main() -> None:
    print("[pgload] duckdb:", DUCKDB_PATH)
    dcon = duckdb.connect(DUCKDB_PATH)

    df: pd.DataFrame = dcon.execute("select * from main.mart_ticket_metrics").fetchdf()
    print(f"[pgload] rows={len(df)} cols={len(df.columns)}")

    TMP.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(TMP, index=False)
    print(f"[pgload] wrote tmp csv: {TMP} ({TMP.stat().st_size} bytes)")

    ddl = f"""
    drop table if exists {TABLE};
    create table {TABLE} (
      request_id text,
      created_ts timestamp,
      first_responded_ts timestamp,
      first_response_minutes bigint,
      priority text,
      request_status text,
      fcr_proxy boolean,
      sla_breach boolean,
      sla_threshold_minutes integer
    );
    """

    with psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    ) as con:
        with con.cursor() as cur:
            cur.execute(ddl)
            with cur.copy(f"COPY {TABLE} FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                cp.write(TMP.read_bytes())
        con.commit()

    print("[pgload] loaded into postgres table:", TABLE)


if __name__ == "__main__":
    main()
