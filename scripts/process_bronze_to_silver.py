import os
import sys
from pathlib import Path

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


def process_bronze_to_silver():
    base_dir = Path(__file__).resolve().parent.parent
    bronze_dir = base_dir / "data" / "bronze"
    silver_dir = base_dir / "data" / "silver" / "weather_cleaned"

    json_files = (
        [str(p.as_posix()) for p in bronze_dir.glob("*.json")]
        if bronze_dir.exists()
        else []
    )

    if not json_files:
        print(f"❌ No JSON files found in {bronze_dir}!")
        return

    # Initialize PySpark Session with NativeIO bypass options
    spark = (
        SparkSession.builder.appName("WeatherBronzeToSilver")
        .master("local[*]")
        # Bypass native Windows permission checks that cause UnsatisfiedLinkError
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(" Reading raw JSON files into PySpark...")
    raw_df = spark.read.option("multiline", "true").json(json_files)

    clean_df = raw_df.select(
        col("city_name").alias("city"),
        col("latitude").cast("double").alias("latitude"),
        col("longitude").cast("double").alias("longitude"),
        col("timezone").alias("timezone"),
        to_timestamp(col("extracted_at")).alias("ingestion_timestamp"),
        to_timestamp(col("current.time")).alias("measurement_timestamp"),
        col("current.temperature_2m").cast("double").alias("temperature_c"),
        col("current.apparent_temperature")
        .cast("double")
        .alias("feels_like_c"),
        col("current.relative_humidity_2m")
        .cast("integer")
        .alias("humidity_pct"),
        col("current.precipitation")
        .cast("double")
        .alias("precipitation_mm"),
        col("current.rain").cast("double").alias("rain_mm"),
        col("current.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
    )

    print("\n Cleaned Schema (Silver Layer):")
    clean_df.printSchema()

    print("\n Preview of Transformed Data:")
    clean_df.show(truncate=False)

    silver_spark_path = str(silver_dir.as_posix())
    print(f"\n Writing cleaned Parquet data to: {silver_spark_path}")

    # Write output
    clean_df.write.mode("overwrite").partitionBy("city").parquet(
        silver_spark_path
    )

    print(" Successfully processed Bronze data to Silver layer!")


if __name__ == "__main__":
    process_bronze_to_silver()