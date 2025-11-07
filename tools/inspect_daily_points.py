import argparse
import os
import duckdb
import pandas as pd


def show_duplicate_points(warehouse_path: str, top_n: int = 20, show_rows: int = 10) -> None:
    """
    Connect to the DuckDB warehouse and print dates that have multiple points in `bubble_metrics`.

    For the top `top_n` dates with the highest multiplicity, prints up to `show_rows`
    rows for each date from `bubble_metrics` and the corresponding rows in
    `hype_index` and `reality_index` so you can inspect where the duplicates come from.
    """

    if not os.path.exists(warehouse_path):
        raise FileNotFoundError(f"Warehouse not found: {warehouse_path}")

    conn = duckdb.connect(warehouse_path)
    try:
        # Count rows per date in bubble_metrics
        counts_q = """
        SELECT date, COUNT(*) as cnt
        FROM bubble_metrics
        GROUP BY date
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, date DESC
        LIMIT ?
        """

        dup_dates = conn.execute(counts_q, [top_n]).fetchdf()

        if dup_dates.empty:
            print("No duplicate dates found in `bubble_metrics` (every date has <= 1 row).")
            return

        pd.set_option('display.width', 160)
        pd.set_option('display.max_columns', None)

        print(f"Found {len(dup_dates)} dates with >1 rows in bubble_metrics (showing top {top_n}):\n")
        print(dup_dates.to_string(index=False))

        for row in dup_dates.itertuples(index=False):
            date = row.date
            cnt = int(row.cnt)
            print("\n" + "=" * 80)
            print(f"Date: {date}  (rows: {cnt})")

            bm_q = """
            SELECT time_id, date, computed_at, hype_index, reality_index
            FROM bubble_metrics
            WHERE date = ?
            ORDER BY computed_at, time_id
            LIMIT ?
            """

            hype_q = """
            SELECT time_id, date, hype_index
            FROM hype_index
            WHERE date = ?
            ORDER BY time_id DESC
            LIMIT ?
            """

            reality_q = """
            SELECT time_id, date, reality_index
            FROM reality_index
            WHERE date = ?
            ORDER BY time_id DESC
            LIMIT ?
            """

            bm_df = conn.execute(bm_q, [date, show_rows]).fetchdf()
            hi_df = conn.execute(hype_q, [date, show_rows]).fetchdf()
            ri_df = conn.execute(reality_q, [date, show_rows]).fetchdf()

            if not bm_df.empty:
                print('\nbubble_metrics rows:')
                print(bm_df.to_string(index=False))
            else:
                print('\nNo rows in bubble_metrics for this date')

            if not hi_df.empty:
                print('\nCorresponding hype_index rows (latest first):')
                print(hi_df.to_string(index=False))
            else:
                print('\nNo rows in hype_index for this date')

            if not ri_df.empty:
                print('\nCorresponding reality_index rows (latest first):')
                print(ri_df.to_string(index=False))
            else:
                print('\nNo rows in reality_index for this date')

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect duplicate daily points in bubble_metrics")
    parser.add_argument("--warehouse", "-w", default=os.path.join("data", "warehouse", "ai_bubble.duckdb"),
                        help="Path to DuckDB warehouse file (default: data/warehouse/ai_bubble.duckdb)")
    parser.add_argument("--top", "-t", type=int, default=20, help="Number of top duplicate dates to show")
    parser.add_argument("--rows", "-r", type=int, default=20, help="Number of rows to show per date")

    args = parser.parse_args()

    show_duplicate_points(args.warehouse, top_n=args.top, show_rows=args.rows)


if __name__ == "__main__":
    main()
