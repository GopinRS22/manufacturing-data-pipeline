"""Shared Spark/Delta path utilities."""
from __future__ import annotations

import os
from pathlib import Path
from pyspark.sql import SparkSession


def path(name: str) -> str:
    return str(Path(os.getenv("DATA_ROOT", "/data")) / name)


def spark_session(app: str) -> SparkSession:
    return (SparkSession.builder.appName(app)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def exists(name: str) -> bool:
    return (Path(path(name)) / "_delta_log").exists()
