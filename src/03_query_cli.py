from __future__ import annotations

import argparse
import sqlite3
import pandas as pd
from utils import DB_DIR


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--top", type=int, default=15, help="How many breweries to show")
    p.add_argument("year", nargs="?", type=int, default=None, help="Filter by year (optional)")
    p.add_argument("--medal", choices=["Gold", "Silver", "Bronze"], default=None, help="Filter by medal (optional)")
    p.add_argument("--sql", default=None, help="Run a custom SQL query and print results")
    args = p.parse_args()

    db_path = DB_DIR / "beer_awards.sqlite"
    conn = sqlite3.connect(db_path)

    try:
        if args.sql:
            df = pd.read_sql_query(args.sql, conn)
            print(df.to_string(index=False))
            return

        where = []
        params: list = []

        if args.year is not None:
            where.append("a.year = ?")
            params.append(args.year)

        if args.medal is not None:
            where.append("a.medal = ?")
            params.append(args.medal)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        q = f"""
        SELECT
            a.brewery_name,
            COUNT(*) AS medals_total,
            SUM(CASE WHEN a.medal='Gold' THEN 1 ELSE 0 END) AS golds,
            SUM(CASE WHEN a.medal='Silver' THEN 1 ELSE 0 END) AS silvers,
            SUM(CASE WHEN a.medal='Bronze' THEN 1 ELSE 0 END) AS bronzes
        FROM awards a
        WHERE a.brewery_name IS NOT NULL AND a.brewery_name <> ''
        GROUP BY a.brewery_name
        ORDER BY medals_total DESC, golds DESC, silvers DESC, bronzes DESC, a.brewery_name ASC
        LIMIT ?
        """

        params.append(args.top)

        df = pd.read_sql_query(q, conn, params=params)
        print(df.to_string(index=False))

    finally:
        conn.close()


if __name__ == "__main__":
    main()