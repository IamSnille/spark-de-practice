from pyspark.sql import SparkSession
from pathlib import Path
spark = (
        SparkSession.builder
        .appName("practice_etl")
        .getOrCreate()
    )
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "data" / "raw" / "weather"
OUTPUT_PATH = BASE_DIR / "data" / "output" / "weather"
OUTPUT_PATH2 = BASE_DIR / "data" / "output" / "result"
parquet_df = spark.read.parquet(str(OUTPUT_PATH))
parquet_df2 = spark.read.parquet(str(OUTPUT_PATH2))
ff1 = parquet_df.count()
ff2 = parquet_df2.count()
print(ff1)
print(ff2)
