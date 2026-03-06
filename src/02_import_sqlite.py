from __future__ import annotations
import re
import sqlite3
import pandas as pd
from utils import DATA_RAW, DB_DIR


def latest_csv(prefix: str) -> str:
    files = sorted(DATA_RAW.glob(f"{prefix}_*.csv"))
    if not files:
        raise SystemExit(f"No files found for {prefix}_*.csv. Run 01_collect_data.py first.")
    return str(files[-1])

_SUFFIXES = r"\b(inc|llc|ltd|co|company|corp|corporation|brewery|brewing|brauerei|gmbh|sarl|sa|bv|ab|oy|plc)\b"
def norm_name(series: pd.Series) -> pd.Series:
    # Normalize for joining/matching (basic)
    return (
        series.astype("string")
        .fillna("")
        .str.lower()
        .str.replace(r"[^\w\s]", " ", regex=True)
        .str.replace(_SUFFIXES, " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
    )

def extract_country_from_location(location:str | None) -> str | None:
    if not isinstance(location, str) or not location.strip():
        return None
    
    parts = [p.strip() for p in location.split(",") if p.strip()]
    if not parts:
        return None
    
    country = parts[-1]

    mapping = {
        "USA": "United States",
        "U.S.A.": "United States",
        "U.S.": "United States",
        "UNITED STATES OF AMERICA": "United States",
        "ENGLAND": "United Kingdom",
        "SCOTLAND": "United Kingdom",
        "WALES": "United Kingdom",
    }

    return mapping.get(country.upper(), country)


def main():
    DB_DIR.mkdir(parents=True, exist_ok=True)
    db_path = DB_DIR / "beer_awards.sqlite"

    wbc_path = latest_csv("wbc_awards")
    bjcp_path = latest_csv("bjcp_styles")
    obdb_path = latest_csv("obdb_breweries")

    wbc = pd.read_csv(wbc_path)
    bjcp = pd.read_csv(bjcp_path)
    obdb = pd.read_csv(obdb_path)

    # --- Always create the normalized columns so indexes never fail ---
    if "brewery_name" not in wbc.columns:
        wbc["brewery_name"] = ""  # placeholder until you parse it in cleaning
    wbc["brewery_name_norm"] = norm_name(wbc["brewery_name"])

    if "location" in wbc.columns:
        wbc["country_wbc"] = wbc["location"].apply(extract_country_from_location)
    else:
        wbc["country_wbc"] = None

    if "brewery_name" not in obdb.columns:
        raise SystemExit("OBDB breweries CSV missing 'brewery_name' column.")
    obdb["brewery_name_norm"] = norm_name(obdb["brewery_name"])

    wbc = wbc.merge(
        obdb[["brewery_name_norm", "country"]],
        how="left",
        on="brewery_name_norm",
        suffixes=("", "_obdb")
    )

    wbc["country_final"] = (wbc["country_wbc"].fillna(wbc["country"]).fillna("Unkown")
        
    )

    # Optional: ensure year is numeric for filtering/sorting
    if "year" in wbc.columns:
        wbc["year"] = pd.to_numeric(wbc["year"], errors="coerce")

    with sqlite3.connect(db_path) as conn:
        # Speed-ish settings for imports (safe for local dev)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        wbc.to_sql("awards", conn, if_exists="replace", index=False)
        bjcp.to_sql("styles", conn, if_exists="replace", index=False)
        obdb.to_sql("breweries", conn, if_exists="replace", index=False)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_awards_brewery ON awards(brewery_name_norm);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_breweries_brewery ON breweries(brewery_name_norm);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_awards_year ON awards(year);")

    print(f"Imported into {db_path}")
    print("Tables: awards, styles, breweries")


if __name__ == "__main__":
    main()