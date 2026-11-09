import os
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Поддерживаемые форматы -> опции чтения
FORMATS = {
    "csv":     {"header": "true", "inferSchema": "true"},
    "json":    {"multiLine": "true"},
    "parquet": {},
    "orc":     {},
}


def flatten_daily(df: DataFrame) -> DataFrame:
    """Разворачивает параллельные массивы daily.* в строки: город + дата."""
    return (
        df.select(
            "city", "latitude", "longitude", "elevation", "timezone", "source_file",
            F.explode(
                F.arrays_zip(
              F.col("daily.time").alias("date"),
                    F.col("daily.temperature_2m_max").alias("temp_max"),
                    F.col("daily.temperature_2m_min").alias("temp_min"),
                    F.col("daily.precipitation_sum").alias("precipitation_sum"),
                )
            ).alias("d"),
        )
        .select(
            "city", "latitude", "longitude", "elevation", "timezone", "source_file",
            F.to_date("d.date").alias("date"),
            F.col("d.temp_max").alias("temp_max"),
            F.col("d.temp_min").alias("temp_min"),
            F.col("d.precipitation_sum").alias("precipitation_sum"),
        )
    )


def read_format(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    """Читает все файлы одного формата по маске *.fmt из папки (или один файл)."""
    target = path if os.path.isfile(path) else os.path.join(path, f"*.{fmt}")
    df = (
        spark.read.format(fmt)
        .options(**FORMATS[fmt])
        .load(target)
        .withColumn("source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
    )
    # Вложенный JSON (Open-Meteo) превращаем в плоскую таблицу
    if fmt == "json" and "daily" in df.columns:
        df = flatten_daily(df)
    return df


def read_input(spark: SparkSession, input_path: str) -> DataFrame:
    """Читает файл или все поддерживаемые файлы из папки."""
    # Один файл: формат по расширению
    if os.path.isfile(input_path):
        ext = os.path.splitext(input_path)[1].lstrip(".").lower()
        if ext not in FORMATS:
            raise ValueError(f"Неподдерживаемый формат: .{ext}")
        return read_format(spark, input_path, ext)

    # Папка: определяем, какие форматы в ней реально есть
    present = {
        os.path.splitext(f)[1].lstrip(".").lower()
        for f in os.listdir(input_path)
    } & set(FORMATS)

    if not present:
        raise FileNotFoundError(f"В {input_path} нет файлов форматов: {list(FORMATS)}")

    frames = [read_format(spark, input_path, fmt) for fmt in sorted(present)]

    # allowMissingColumns: недостающие колонки заполняются null
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_path = os.path.join(base_dir, "data", "raw", "weather")
    output_path = os.path.join(base_dir, "data", "output", "result")

    spark = SparkSession.builder.appName("weather_etl").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = read_input(spark, input_path)

    # ... твоя обработка данных здесь ...

    df.write.mode("overwrite").parquet(output_path)
    print(f"Результат сохранён: {output_path}")

    df = read_input(spark, input_path)
    df.printSchema()
    df.show(5, truncate=False)

    spark.stop()




if __name__ == "__main__":
    main()