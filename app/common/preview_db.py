import argparse

from src.storage.db import get_connection

PREVIEW_ROWS = 5
EXCLUDE_COLUMNS = {"content", "content_html"}


def preview_table(
    schema: str,
    table: str,
    n: int = PREVIEW_ROWS,
    select_columns: list[str] | None = None,
) -> None:
    qualified = f"{schema}.{table}"
    with get_connection() as conn:
        available = [
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                [schema, table],
            ).fetchall()
        ]

        if not available:
            print(f"Table '{qualified}' not found or has no columns.")
            return

        if select_columns:
            unknown = [c for c in select_columns if c not in available]
            if unknown:
                print(f"Unknown column(s): {', '.join(unknown)}")
                print(f"Available columns: {', '.join(available)}")
                return
            columns = select_columns
        else:
            columns = [c for c in available if c not in EXCLUDE_COLUMNS]

        col_list = ", ".join(columns)
        rows = conn.execute(f"SELECT {col_list} FROM {qualified} LIMIT ?", [n]).fetchall()

        print(f"\n=== {qualified} ({len(rows)} of first {n} rows, {len(columns)} columns) ===")
        print("  ".join(f"{c:<30}" for c in columns))
        print("-" * (32 * len(columns)))
        for row in rows:
            print("  ".join(f"{str(v):<30}" for v in row))


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview rows from a DuckDB table.")
    parser.add_argument("table", help="Qualified table name, e.g. headfi.raw_pages")
    parser.add_argument("-n", type=int, default=PREVIEW_ROWS, help="Number of rows to display")
    parser.add_argument(
        "--select-columns",
        nargs="+",
        metavar="COL",
        help="Columns to display (defaults to all non-excluded columns)",
    )
    args = parser.parse_args()

    parts = args.table.split(".", 1)
    if len(parts) != 2:
        parser.error("Table must be fully qualified as schema.table")

    preview_table(parts[0], parts[1], n=args.n, select_columns=args.select_columns)


if __name__ == "__main__":
    main()
