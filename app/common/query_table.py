import argparse

from src.storage.db import get_connection


def query_table(query: str) -> None:
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        for row in rows:
            print("  ".join(f"{str(v):<30}" for v in row))


def main() -> None:
    parser = argparse.ArgumentParser(description="Query a DuckDB table.")
    parser.add_argument("query", help="DuckDB query")
    args = parser.parse_args()

    query_table(args.query)


if __name__ == "__main__":
    main()
