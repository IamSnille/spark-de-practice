from pathlib import Path
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, arrays_zip, explode, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "data" / "raw" / "weather"
OUTPUT_PATH = BASE_DIR / "data" / "output" / "weather"

weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("daily", StructType([
        StructField("time", ArrayType(StringType()), True),
        StructField("temperature_2m_max", ArrayType(DoubleType()), True),
        StructField("temperature_2m_min", ArrayType(DoubleType()), True),
        StructField("precipitation_sum", ArrayType(DoubleType()), True),
    ]), True),
])


def get_folder_size(folder) -> int:
    """Total size of all files in a folder (bytes)."""
    total_size = 0
    for dirpath, _, filenames in os.walk(folder):
        for filename in filenames:
            total_size += (Path(dirpath) / filename).stat().st_size
    return total_size


spark = (
    SparkSession.builder
    .appName("WeatherIngestion")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Read raw JSON
weather_df = (
    spark.read
    .schema(weather_schema)
    .option("multiLine", True)
    .json(str(RAW_PATH))
)
weather_df.printSchema()
weather_df.show(3, truncate=False)

# Flatten: one row per city per day
weather_flat_df = (
    weather_df
    .select(
        "city", "latitude", "longitude",
        explode(
            arrays_zip(
                col("daily.time").alias("date"),
                col("daily.temperature_2m_max").alias("temp_max"),
                col("daily.temperature_2m_min").alias("temp_min"),
                col("daily.precipitation_sum").alias("precipitation_mm"),
            )
        ).alias("d"),
    )
    .select(
        "city", "latitude", "longitude",
        to_date(col("d.date")).alias("date"),
        col("d.temp_max").alias("temp_max"),
        col("d.temp_min").alias("temp_min"),
        col("d.precipitation_mm").alias("precipitation_mm"),
    )
)
weather_flat_df.printSchema()
weather_flat_df.show(10, truncate=False)
print("Total rows:", weather_flat_df.count())

# Write Parquet (partitioned by date)
weather_flat_df.write.mode("overwrite").partitionBy("date").parquet(str(OUTPUT_PATH))
print(f"Saved Parquet data to: {OUTPUT_PATH}")

# Read back and verify
parquet_df = spark.read.parquet(str(OUTPUT_PATH))
parquet_df.printSchema()
print("Total rows:", parquet_df.count())

# Partition pruning demo: filter by the first date found in the data
sample_date = parquet_df.agg({"date": "min"}).collect()[0][0]
filtered_weather_df = parquet_df.filter(col("date") == sample_date)
filtered_weather_df.explain(True)

# Size comparison: raw JSON vs Parquet
raw_size = get_folder_size(RAW_PATH)
parquet_size = get_folder_size(OUTPUT_PATH)
print(f"JSON size: {raw_size / 1024:.2f} KB")
print(f"Parquet size: {parquet_size / 1024:.2f} KB")

spark.stop()