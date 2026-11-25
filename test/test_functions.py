
import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, ArrayType
)
from pyspark.sql.functions import (
    col,
    arrays_zip,
    explode,
    to_date,
    round
)

from functions import (
    _check_format, read_data, clean,
    transform, write_data
)


# Input schema for clean()
NESTED_SCHEMA = StructType([
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


# --- _check_format ---

@pytest.mark.parametrize("fmt", ["csv", "json", "parquet", "orc"])
def test_check_format_valid(fmt):
    assert _check_format(fmt) == fmt


def test_check_format_uppercase():
    assert _check_format("JSON") == "json"


def test_check_format_invalid():
    with pytest.raises(ValueError):
        _check_format("xml")


# --- read_data ---

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

    assert weather_df.columns == [
        "city", "latitude", "longitude", "timezone", "daily"
    ]

    rows = weather_df.collect()

    assert len(rows) == 1
    assert rows[0]["city"] == "Warsaw"
    assert rows[0]["daily"]["time"] == ["2026-11-15", "2026-11-16"]


def test_read_data_parquet(spark, tmp_path):
    filepath = str(tmp_path / "data_parquet")

    source_df = spark.createDataFrame(
        [("Warsaw", 10.5), ("Kyiv", 20.0)],
        ["city", "temperature"]
    )

    source_df.write.parquet(filepath)

    weather_df = read_data(spark, filepath, "parquet")

    assert set(weather_df.columns) == {"city", "temperature"}
    assert weather_df.count() == 2
    assert weather_df.exceptAll(source_df).count() == 0

# --- clean ---

def test_clean(spark):
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


    result_df = clean(spark.createDataFrame(data, NESTED_SCHEMA))
    assert result_df.columns == [
        "city", "latitude", "longitude", "date",
        "temperature_max", "temperature_min", "precipitation"
    ]

    rows = result_df.orderBy("date").collect()

    assert len(rows) == 2

    assert rows[0]["city"] == "Warsaw"
    assert str(rows[0]["date"]) == "2026-11-15"
    assert rows[0]["temperature_max"] == 10.0
    assert rows[0]["temperature_min"] == 3.0
    assert rows[0]["precipitation"] == 1.5

    assert str(rows[1]["date"]) == "2026-11-16"
    assert rows[1]["temperature_max"] == 12.0
    assert rows[1]["temperature_min"] == 5.0
    assert rows[1]["precipitation"] == 0.0


def test_clean_drops_row_without_city(spark):
    daily = (["2026-11-15"], [10.0], [3.0], [1.5])

    data = [
        ("Warsaw", 52.2297, 21.0122, daily),
        (None, 50.4501, 30.5234, daily),
    ]

    result_df = clean(spark.createDataFrame(data, NESTED_SCHEMA))

    assert [row["city"] for row in result_df.collect()] == ["Warsaw"]


def test_clean_drops_day_with_missing_value(spark):
    data = [
        (
            "Warsaw", 52.2297, 21.0122,
            (
                ["2026-11-15", "2026-11-16"],
                [10.0, None],
                [3.0, 5.0],
                [1.5, 0.0]
            )
        )
    ]

    result_df = clean(spark.createDataFrame(data, NESTED_SCHEMA))
    result_df.show()
    rows = result_df.collect()

    assert len(rows) == 1
    assert str(rows[0]["date"]) == "2026-11-15"


# --- transform ---

def test_transform(spark):
    data = [
        ("Warsaw", 10.0, 2.0),
        ("Kyiv", 20.0, 10.0),
        ("Lviv", 10.123, 2.0)
    ]

    columns = ["city", "temperature_max", "temperature_min"]

    result_df = transform(spark.createDataFrame(data, columns))

    assert result_df.count() == 3
    assert "temperature_avg" in result_df.columns

    result = {
        row["city"]: row["temperature_avg"]
        for row in result_df.collect()
    }

    assert result["Warsaw"] == pytest.approx(6.0)
    assert result["Kyiv"] == pytest.approx(15.0)
    assert result["Lviv"] == pytest.approx(6.06)


# --- write_data ---

def test_write_data_parquet_partitioned_by_date(spark, tmp_path):
    data = [
        ("Warsaw", "2026-11-15", 10.0),
        ("Kyiv", "2026-11-16", 20.0)
    ]

    weather_df = spark.createDataFrame(
        data, ["city", "date", "temperature"]
    )

    filepath = tmp_path / "weather_output"

    write_data(weather_df, str(filepath), "parquet")

    assert (filepath / "date=2026-11-15").is_dir()
    assert (filepath / "date=2026-11-16").is_dir()

    result_df = spark.read.parquet(str(filepath))

    assert result_df.count() == 2
    assert set(result_df.columns) == {"city", "temperature", "date"}
    assert {row["city"] for row in result_df.collect()} == {"Warsaw", "Kyiv"}


def test_write_data_json(spark, tmp_path):
    weather_df = spark.createDataFrame(
        [("Warsaw", "2026-11-15", 10.0)],
        ["city", "date", "temperature"]
    )

    filepath = tmp_path / "json_output"

    write_data(weather_df, str(filepath), "json")

    result_df = spark.read.json(str(filepath))

    assert result_df.count() == 1
    assert set(result_df.columns) == {"city", "date", "temperature"}

    row = result_df.collect()[0]

    assert row["city"] == "Warsaw"
    assert row["temperature"] == 10.0


def test_write_data_invalid_format(spark, tmp_path):
    weather_df = spark.createDataFrame(
        [("Warsaw", 10.0)],
        ["city", "temperature"]
    )

    with pytest.raises(ValueError):
        write_data(weather_df, str(tmp_path / "out"), "xml")
