"""Dagster code location package

This re-exports `defs` so you can run commands like:
  - `dagster dev -m dagster_src`
"""

from dagster_src.definitions import defs  # noqa: F401
