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

    All file paths are passed explicitly by the caller -- the resource holds
    only MinIO connection details, bucket, and the default object key for the
    DB file.
    """

    endpoint: str
    access_key: str
    secret_key: str
    bucket: str = "news-aggregator"
    object_key: str = "data/news.db"

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

    def download(self, local_path: str) -> str:
        """
        Downloads the DB file from MinIO to *local_path* and returns it.

        On first run the object won't exist; this is expected and results in
        DuckDB creating a fresh file on first connect.  Any other S3 or
        network error propagates immediately and fails the step.
        """
        client = self._client()
        try:
            client.fget_object(self.bucket, self.object_key, local_path)
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchBucket"):
                return local_path
            raise
        return local_path

    def upload(self, local_path: str, object_key: str | None = None) -> None:
        """
        Uploads a local file to MinIO.

        *object_key* defaults to ``self.object_key`` (the DB file).  Pass an
        explicit key to upload arbitrary files (e.g. export artifacts).
        Creates the bucket on first use.
        """
        client = self._client()
        if not client.bucket_exists(self.bucket):
            client.make_bucket(self.bucket)
        client.fput_object(self.bucket, object_key or self.object_key, local_path)

    @contextmanager
    def get_connection(
        self, db_path: str
    ) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Opens a DuckDB connection to the file at *db_path*.
        """
        conn = duckdb.connect(db_path)
        try:
            yield conn
        finally:
            conn.close()
