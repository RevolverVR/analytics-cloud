import os
import duckdb
import pandas as pd

from analytics_cloud.ingest.read_sheet import read_sheet_preview


def normalize_header(cols):
    out = []
    for c in cols:
        c2 = c.strip().lower()
        c2 = c2.replace(" ", "_")
        c2 = c2.replace("/", "_")
        c2 = c2.replace("-", "_")
        while "__" in c2:
            c2 = c2.replace("__", "_")
        out.append(c2)
    return out


def main():
    duckdb_path = os.getenv("DUCKDB_PATH", "data/dev.duckdb")
    table = os.getenv("RAW_TABLE_NAME", "raw_base_tickets")

    values = read_sheet_preview()
    if not values or len(values) < 2:
        raise RuntimeError("Sheet returned no data (or only header).")

    header = values[0]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=header)
    df.columns = normalize_header(df.columns)

    # RAW: keep everything as string, we’ll cast in dbt staging
    df = df.astype("string")

    os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
    con = duckdb.connect(duckdb_path)

    con.execute(f"create or replace table {table} as select * from df")

    count = con.execute(f"select count(*) from {table}").fetchone()[0]
    print(f"[duckdb] wrote table={table} rows={count} path={duckdb_path}")

    print("[duckdb] sample:")
    print(con.execute(f"select * from {table} limit 3").fetchdf())

    con.close()


if __name__ == "__main__":
    main()
