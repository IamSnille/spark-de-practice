
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, ArrayType
)

from src.functions import (
    _check_format, read_data, clean,
    transform, write_data
)


# Tests valid and invalid file formats.
@pytest.mark.parametrize(
    "fmt",
    ["csv", "json", "parquet", "orc"]
)
def test_check_format_valid(fmt):
    assert _check_format(fmt) == fmt


def test_check_format_uppercase():
    assert _check_format("JSON") == "json"


def test_check_format_invalid():
    with pytest.raises(ValueError):
        _check_format("xml")


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_functions")
        .getOrCreate()
    )

    yield spark

    spark.stop()


# Tests reading data from a JSON file.
def test_read_data_json(spark, tmp_path):
    filepath = str(tmp_path / "weather.json")

    with open(filepath, "w", encoding="utf-8") as file:
        file.write("""
        {
            "city": "Warsaw",
            "latitude": 52.2297,
            "longitude": 21.0122,
            "timezone": "Europe/Warsaw",
            "daily": {
                "time": ["2026-11-15", "2026-11-16"],
                "temperature_2m_max": [10.0, 12.0],
                "temperature_2m_min": [3.0, 5.0],
                "precipitation_sum": [1.5, 0.0]
            }
        }
        """)

    weather_df = read_data(spark, filepath, "json")

    assert weather_df.count() == 1
    assert weather_df.columns == [
        "city", "latitude", "longitude", "timezone", "daily"
    ]


# Tests cleaning and flattening nested JSON weather data.
def test_clean(spark):
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("daily", StructType([
            StructField("time", ArrayType(StringType()), True),
            StructField("temperature_2m_max", ArrayType(DoubleType()), True),
            StructField("temperature_2m_min", ArrayType(DoubleType()), True),
            StructField("precipitation_sum", ArrayType(DoubleType()), True)
        ]), True)
    ])

    data = [
        (
            "Warsaw", 52.2297, 21.0122,
            (
                ["2026-11-15", "2026-11-16"],
                [10.0, 12.0],
                [3.0, 5.0],
                [1.5, 0.0]
            )
        )
    ]

    weather_df = spark.createDataFrame(data, schema)

    result_df = clean(weather_df)

    assert result_df.columns == [
        "city",
        "latitude",
        "longitude",
        "date",
        "temperature_max",
        "temperature_min",
        "precipitation"
    ]

    rows = result_df.orderBy("date").collect()

    assert len(rows) == 2

    assert rows[0]["city"] == "Warsaw"
    assert str(rows[0]["date"]) == "2026-11-15"
    assert rows[0]["temperature_max"] == 10.0
    assert rows[0]["temperature_min"] == 3.0
    assert rows[0]["precipitation"] == 1.5

    assert rows[1]["city"] == "Warsaw"
    assert str(rows[1]["date"]) == "2026-11-16"
    assert rows[1]["temperature_max"] == 12.0
    assert rows[1]["temperature_min"] == 5.0
    assert rows[1]["precipitation"] == 0.0


# Tests calculating the average temperature.
def test_transform(spark):
    data = [
        ("Warsaw", 10.0, 2.0),
        ("Kyiv", 20.0, 10.0),
        ("Lviv", 10.123, 2.0)
    ]

    columns = [
        "city",
        "temperature_max",
        "temperature_min"
    ]

    weather_df = spark.createDataFrame(data, columns)

    result_df = transform(weather_df)

    result = {
        row["city"]: row["temperature_avg"]
        for row in result_df.collect()
    }

    assert result["Warsaw"] == 6.0
    assert result["Kyiv"] == 15.0
    assert result["Lviv"] == 6.06


# Tests writing data to Parquet and reading it back.
def test_write_data(spark, tmp_path):
    data = [
        ("Warsaw", "2026-11-15", 10.0),
        ("Kyiv", "2026-11-15", 20.0)
    ]

    columns = ["city", "date", "temperature"]

    weather_df = spark.createDataFrame(data, columns)

    filepath = str(tmp_path / "weather_output")

    write_data(weather_df, filepath, "parquet")

    result_df = spark.read.parquet(filepath)

    assert result_df.count() == 2
    assert set(result_df.columns) == {
        "city", "temperature", "date"
    }

    rows = result_df.collect()

    assert len(rows) == 2
    assert {row["city"] for row in rows} == {
        "Warsaw", "Kyiv"
    }
