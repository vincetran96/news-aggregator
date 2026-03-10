import argparse

from src.storage.db import get_connection

PREVIEW_ROWS = 5
EXCLUDE_COLUMNS = {"content", "content_html"}
VALID_DIRECTIONS = {"asc", "desc"}


def _parse_order_terms(
    raw: list[str],
    available: list[str],
) -> tuple[list[tuple[str, str]], str | None]:
    """
    Parses raw order-by tokens into (column, direction) pairs and builds
    the SQL ORDER BY clause. Returns (terms, error_message).

    Each token is expected to be "column" or "column asc|desc".
    """
    terms: list[tuple[str, str]] = []
    for token in raw:
        parts = token.strip().split()
        col = parts[0]
        direction = parts[1].lower() if len(parts) > 1 else "asc"

        if col not in available:
            return [], f"Unknown order-by column: '{col}'. Available: {', '.join(available)}"
        if direction not in VALID_DIRECTIONS:
            return [], f"Invalid direction '{direction}' for column '{col}'. Use asc or desc."

        terms.append((col, direction.upper()))

    return terms, None


def preview_table(
    schema: str,
    table: str,
    n: int = PREVIEW_ROWS,
    select_columns: list[str] | None = None,
    order_by: list[str] | None = None,
) -> None:
    qualified = f"{schema}.{table}"
    with get_connection() as conn:
        available_cols = [
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                [schema, table],
            ).fetchall()
        ]

        if not available_cols:
            print(f"Table '{qualified}' not found or has no columns.")
            return

        if select_columns:
            unknown_cols = [c for c in select_columns if c not in available_cols]
            if unknown_cols:
                print(f"Unknown column(s): {', '.join(unknown_cols)}")
                print(f"Available columns: {', '.join(available_cols)}")
                return
            columns = select_columns
        else:
            columns = [c for c in available_cols if c not in EXCLUDE_COLUMNS]

        order_clause = ""
        order_summary = ""
        if order_by:
            terms, error = _parse_order_terms(order_by, available_cols)
            if error:
                print(error)
                return
            order_clause = "ORDER BY " + ", ".join(f"{col} {direction}" for col, direction in terms)
            order_summary = " | " + ", ".join(f"{col} {direction}" for col, direction in terms)

        col_list = ", ".join(columns)
        rows = conn.execute(
            f"SELECT {col_list} FROM {qualified} {order_clause} LIMIT ?", [n]
        ).fetchall()

        print(
            f"\n=== {qualified} ({len(rows)} of first {n} rows, {len(columns)} columns{order_summary}) ==="
        )
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
    parser.add_argument(
        "--order-by",
        nargs="+",
        metavar="COL [asc|desc]",
        help='Columns to sort by, e.g. --order-by "insert_tstamp asc" "page_num desc"',
    )
    args = parser.parse_args()

    parts = args.table.split(".", 1)
    if len(parts) != 2:
        parser.error("Table must be fully qualified as schema.table")

    preview_table(
        parts[0],
        parts[1],
        n=args.n,
        select_columns=args.select_columns,
        order_by=args.order_by,
    )


if __name__ == "__main__":
    main()
