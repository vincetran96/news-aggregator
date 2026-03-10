import argparse

from src.storage.db import get_connection


def drop_table(schema: str, table: str) -> None:
    qualified = f"{schema}.{table}"
    with get_connection() as conn:
        exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()

        if not exists or exists[0] == 0:
            print(f"Table '{qualified}' does not exist.")
            return

        confirm = input(f"Drop table '{qualified}'? This cannot be undone. [y/N] ")
        if confirm.strip().lower() != "y":
            print("Aborted.")
            return

        conn.execute(f"DROP TABLE {qualified}")
        print(f"Dropped '{qualified}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Drop a DuckDB table by fully qualified name.")
    parser.add_argument("table", help="Qualified table name, e.g. headfi.raw_pages")
    args = parser.parse_args()

    parts = args.table.split(".", 1)
    if len(parts) != 2:
        print("Table must be fully qualified as schema.table")
        return

    drop_table(parts[0], parts[1])


if __name__ == "__main__":
    main()
