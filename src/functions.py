import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Snille\PycharmProjects\spark-de-practice\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Snille\PycharmProjects\spark-de-practice\.venv\Scripts\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, mean, expr

SUPPORTED_FORMATS = {"csv", "json", "parquet", "orc"}


def _check_format(fmt: str) -> str:
    fmt = fmt.lower()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Формат '{fmt}' не поддерживается. Доступные: {sorted(SUPPORTED_FORMATS)}"
        )
    return fmt


# --- 1. ФУНКЦИЯ ЧТЕНИЯ ---
def read_data(spark: SparkSession, filepath: str, fmt: str = "csv", **options) -> DataFrame:
    """Читает исходный файл в заданном формате (csv, json, parquet, orc)"""
    fmt = _check_format(fmt)
    if fmt == "csv":
        options = {"header": True, "inferSchema": True, **options}
    df = spark.read.format(fmt).options(**options).load(filepath)
    print(f"✓ Файл прочитан: {filepath} (формат: {fmt})")
    print(f"  Строк: {df.count()}, Колонок: {len(df.columns)}")
    return df


# --- 2. ФУНКЦИЯ ОЧИСТКИ ---

def clean(df: DataFrame) -> DataFrame:
    """Удаляет невалидные строки и приводит типы"""
    rows_before = df.count()
    df = df.dropna()
    df = df.withColumn("amount", expr("try_cast(amount as double)"))
    df = df.withColumn("date", expr("try_cast(date as date)"))
    df = df.dropna()
    rows_after = df.count()
    print(f"✓ Данные очищены")
    print(f"  Удалено строк: {rows_before - rows_after}")
    return df


# --- 3. ФУНКЦИЯ ТРАНСФОРМАЦИИ ---
def transform(df: DataFrame) -> DataFrame:
    """Вычисления и агрегации"""
    df = df.withColumn("total", col("amount") * col("quantity"))
    df.cache()  # Кэшируем перед агрегацией
    result = df.groupBy("category").agg(
        spark_sum("total").alias("total_sum"),
        mean("amount").alias("avg_amount")
    )
    print(f"✓ Трансформация выполнена")
    print(f"  Уникальных категорий: {result.count()}")
    return result


# --- 4. ФУНКЦИЯ ЗАПИСИ ---
def write_data(df: DataFrame, filepath: str, fmt: str = "csv", **options) -> None:
    """Сохраняет результат в заданном формате (csv, json, parquet, orc)"""
    fmt = _check_format(fmt)
    if fmt == "csv":
        options = {"header": True, **options}
    df.coalesce(1).write.format(fmt).mode("overwrite").options(**options).save(filepath)
    print(f"✓ Данные сохранены в {filepath} (формат: {fmt})")
