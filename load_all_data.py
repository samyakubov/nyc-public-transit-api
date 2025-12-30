import duckdb
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")
DB_PATH = "transit.duckdb"

print(f"Connecting to database at {DB_PATH}")
con = duckdb.connect(DB_PATH)

for feed_dir in DATA_DIR.iterdir():
    if not feed_dir.is_dir():
        print(f"Skipping non-directory {feed_dir}")
        continue

    print(f"Processing feed directory: {feed_dir}")

    for txt_file in feed_dir.glob("*.txt"):
        table = txt_file.stem
        print(f"Reading file: {txt_file}")

        df = pd.read_csv(txt_file, dtype=str)

        if table == "trips":
            df = df.drop(columns=["block_id"], errors="ignore")
        elif table == "stops":
            df = df.drop(columns=["zone_id", "stop_url", "stop_desc", "parent_station"], errors="ignore")
        elif table == "stop_times":
            cols_to_drop = ["pickup_type", "drop_off_type", "timepoint", "departure_time", "arrival_time"]
            df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
            print(f"Dropped columns {cols_to_drop} for 'stop_times'")

        print(f"Read {len(df)} rows into DataFrame for table '{table}'")

        table_exists = con.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{table}'"
        ).fetchone()[0]

        if not table_exists:
            print(f"Creating table '{table}'")
            col_defs = ", ".join(f"{c} VARCHAR" for c in df.columns)
            con.execute(f"CREATE TABLE {table} ({col_defs})")

        cols = ", ".join(df.columns)
        print(f"Inserting data into table '{table}'")
        con.execute(f"INSERT INTO {table} ({cols}) SELECT * FROM df")

print("Closing database connection")
con.close()
print("Done")
