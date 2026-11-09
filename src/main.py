import os

from pyspark.sql import SparkSession

from functions import read_data, clean, transform, write_data


def main():
    # Project paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    input_path = os.path.join(base_dir, "data", "raw", "weather")
    output_path = os.path.join(base_dir, "data", "output", "result")

    # File formats
    input_fmt = "json"
    output_fmt = "parquet"

    # Python environment
    python_path = os.path.join(
        base_dir, "venv", "Scripts", "python.exe"
    )
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

    # Spark session
    spark = (
        SparkSession.builder
        .appName("practice_etl")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = read_data(spark, input_path, input_fmt)
        df = clean(df)
        result_df = transform(df)
        write_data(result_df, output_path, output_fmt)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
