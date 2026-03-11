import argparse

from src.storage.db import get_connection


def execute_query(query: str) -> None:
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        for row in rows:
            print("  ".join(f"{str(v):<30}" for v in row))


def main() -> None:
    parser = argparse.ArgumentParser(description="Query a DuckDB table.")
    parser.add_argument("query", help="DuckDB query")
    args = parser.parse_args()

    execute_query(args.query)


if __name__ == "__main__":
    main()
