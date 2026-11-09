
import os
import sys

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    ArrayType
)
from pyspark.sql.functions import (
    col,
    arrays_zip,
    explode,
    to_date,
    round
)

SUPPORTED_FORMATS = {"csv", "json", "parquet", "orc"}

# Explicit schema for Open-Meteo JSON
WEATHER_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("daily", StructType([
        StructField("time", ArrayType(StringType()), True),
        StructField("temperature_2m_max", ArrayType(DoubleType()), True),
        StructField("temperature_2m_min", ArrayType(DoubleType()), True),
        StructField("precipitation_sum", ArrayType(DoubleType()), True)
    ]), True)
])


def _check_format(fmt: str) -> str:
    fmt = fmt.lower()

    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Format '{fmt}' not supported. Available: {sorted(SUPPORTED_FORMATS)}"
        )

    return fmt


# --- 1. READ ---
def read_data(
    spark: SparkSession,
    filepath: str,
    fmt: str = "csv",
    **options
) -> DataFrame:
    """Reads the source file in the given format."""

    fmt = _check_format(fmt)

    if fmt == "csv":
        options = {"header": True, "inferSchema": True, **options}
        df = spark.read.format(fmt).options(**options).load(filepath)

    elif fmt == "json":
        options = {"multiLine": True, **options}
        df = (
            spark.read
            .schema(WEATHER_SCHEMA)
            .options(**options)
            .json(filepath)
        )

    else:
        df = spark.read.format(fmt).options(**options).load(filepath)

    print(f"✓ File is read: {filepath} (format: {fmt})")
    print(f"  Rows: {df.count()}, Columns: {len(df.columns)}")

    return df


# --- 2. CLEAN ---
def clean(df: DataFrame) -> DataFrame:

    df = df.dropna(subset=["city", "daily"])

    df = df.withColumn(
        "daily_weather",
        explode(
            arrays_zip(
                col("daily.time"),
                col("daily.temperature_2m_max"),
                col("daily.temperature_2m_min"),
                col("daily.precipitation_sum")
            )
        )
    )

    df = df.select(
        col("city"),
        col("latitude"),
        col("longitude"),
        to_date(col("daily_weather.time")).alias("date"),
        col("daily_weather.temperature_2m_max").alias("temperature_max"),
        col("daily_weather.temperature_2m_min").alias("temperature_min"),
        col("daily_weather.precipitation_sum").alias("precipitation")
    )

    df = df.dropna()

    print("✓ Weather data cleaned and flattened")

    return df


# --- 3. TRANSFORM ---
def transform(df: DataFrame) -> DataFrame:

    df = df.withColumn(
        "temperature_avg",
        round(
            (col("temperature_max") + col("temperature_min")) / 2,
            2
        )
    )

    print("✓ Transformation is complete")
    print(f"  Rows: {df.count()}")

    return df


# --- 4. WRITE ---
def write_data(
    df: DataFrame,
    filepath: str,
    fmt: str = "parquet",
    **options
) -> None:

    fmt = _check_format(fmt)

    writer = (
        df.write
        .mode("overwrite")
        .format(fmt)
        .options(**options)
    )

    if fmt == "parquet":
        writer.partitionBy("date").save(filepath)
    else:
        writer.save(filepath)

    print(f"✓ Data is saved in {filepath} (format: {fmt})")
