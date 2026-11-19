import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """One SparkSession shared by all tests (starting Spark is slow)."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_functions")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield session

    session.stop()
