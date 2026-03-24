"""
Dagster resource that round-trips a DuckDB database file through MinIO.

Download happens once at the start of the pipeline (called explicitly by the
first asset). Upload happens once at the end (called explicitly by the last
asset). If any step fails in between, the upload never executes and MinIO
retains the state from the last successful run.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

import duckdb
from dagster import ConfigurableResource
from minio import Minio
from minio.error import S3Error


class MinIODuckDBResource(ConfigurableResource):
    """
    Manages a DuckDB file backed by MinIO object storage.
    """

    endpoint: str
    access_key: str
    secret_key: str
    bucket: str = "news-aggregator"
    object_key: str = "data/news.db"
    local_path: str = "/tmp/news.db"

    def _client(self) -> Minio:
        raw = self.endpoint
        host = raw.replace("http://", "").replace("https://", "")
        secure = raw.startswith("https://")
        return Minio(
            host,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=secure,
        )

    def download(self) -> None:
        """
        Download the DB file from MinIO to local_path.

        On first run the object won't exist; this is expected and results in
        DuckDB creating a fresh file on first connect.  Any other S3 or
        network error propagates immediately and fails the step.
        """
        client = self._client()
        try:
            client.fget_object(self.bucket, self.object_key, self.local_path)
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchBucket"):
                return
            raise

    def upload(self) -> None:
        """
        Upload the DB file from local_path to MinIO.

        Creates the bucket on first use.  Any S3 or network error propagates
        immediately and fails the step.
        """
        client = self._client()
        if not client.bucket_exists(self.bucket):
            client.make_bucket(self.bucket)
        client.fput_object(self.bucket, self.object_key, self.local_path)

    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Open a DuckDB connection to the local file."""
        conn = duckdb.connect(self.local_path)
        try:
            yield conn
        finally:
            conn.close()
