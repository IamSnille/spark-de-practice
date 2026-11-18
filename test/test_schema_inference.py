import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Snille\PycharmProjects\spark-de-practice\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Snille\PycharmProjects\spark-de-practice\.venv\Scripts\python.exe"

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    ArrayType
)

weather_schema = StructType([
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

from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.functions import col, arrays_zip, explode, to_date

spark = SparkSession.builder \
    .appName("WeatherIngestion") \
    .master("local[*]") \
    .getOrCreate()

weather_df = spark.read \
    .option("multiLine", True) \
    .json(str(Path(__file__).resolve().parent.parent / "data" / "raw" / "weather"))

weather_df.printSchema()
weather_df.show(3, truncate=False)

spark.stop()
#When reading JSON without an explicit schema,
#Spark automatically infers the schema and detects all available fields.
#Schema inference is slower because Spark needs to inspect the input data,
#and it can be risky because inferred data types may be inconsistent or unexpected.
#При чтении JSON без явного указания схемы Spark автоматически определяет схему
#и обнаруживает все доступные поля. Определение схемы происходит медленнее, поскольку Spark
#необходимо проверить входные данные, и может быть рискованным, так как определяемые типы данных
#могут быть непоследовательными или неожиданными.

# 1. Extract arrays from daily
'''weather_arrays_df = weather_df.select(
    col("city"),
    col("latitude"),
    col("longitude"),
    col("daily.time").alias("date"),
    col("daily.temperature_2m_max").alias("temp_max"),
    col("daily.temperature_2m_min").alias("temp_min"),
    col("daily.precipitation_sum").alias("precipitation_mm")
)

# 2. Zip arrays together
weather_zipped_df = weather_arrays_df.withColumn(
    "daily_data",
    arrays_zip(
        col("date"),
        col("temp_max"),
        col("temp_min"),
        col("precipitation_mm")
    )
)

# 3. Explode zipped array
weather_exploded_df = weather_zipped_df.withColumn(
    "daily_data",
    explode(col("daily_data"))
)

# 4. Select final columns and convert date
weather_flat_df = weather_exploded_df.select(
    col("city"),
    col("latitude"),
    col("longitude"),
    to_date(col("daily_data.date")).alias("date"),
    col("daily_data.temp_max").alias("temp_max"),
    col("daily_data.temp_min").alias("temp_min"),
    col("daily_data.precipitation_mm").alias("precipitation_mm")
)

weather_flat_df.printSchema()
weather_flat_df.show(10, truncate=False)

print("Total rows:", weather_flat_df.count())

output_path = Path(__file__).resolve().parent.parent / "data" / "output" / "weather"

weather_flat_df.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(str(output_path))

print(f"Saved Parquet data to: {output_path}")

parquet_df = spark.read.parquet(str(output_path))

parquet_df.printSchema()

print("Total rows:", parquet_df.count())

filtered_weather_df = parquet_df.filter(
    col("date") == "2026-10-01"
)

filtered_weather_df.explain(True)

raw_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "weather"

def get_folder_size(folder):
    total_size = 0

    for dirpath, dirnames, filenames in os.walk(folder):
        for filename in filenames:
            file_path = Path(dirpath) / filename
            total_size += file_path.stat().st_size

    return total_size


raw_size = get_folder_size(raw_path)
parquet_size = get_folder_size(output_path)

print(f"JSON size: {raw_size / 1024:.2f} KB")
print(f"Parquet size: {parquet_size / 1024:.2f} KB")

spark.stop()'''
