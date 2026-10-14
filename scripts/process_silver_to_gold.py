import os
import sys
from pathlib import Path

# Windows Hadoop configuration
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process_silver_to_gold():
    # 1. Resolve absolute paths based on project root
    base_dir = Path(__file__).resolve().parent.parent
    silver_dir = base_dir / "data" / "silver" / "weather_cleaned"
    gold_dir = base_dir / "data" / "gold" / "daily_weather_summary"

    if not silver_dir.exists():
        print(f"❌ Silver directory does not exist at: {silver_dir}")
        print("💡 Run 'python scripts/process_bronze_to_silver.py' first.")
        return

    # 2. Initialize PySpark Session with Windows NativeIO bypass
    spark = (
        SparkSession.builder.appName("WeatherSilverToGold")
        .master("local[*]")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    silver_spark_path = str(silver_dir.as_posix())
    print(f" Reading Silver Parquet dataset from: {silver_spark_path}")

    # Read the partitioned Parquet files from Silver
    silver_df = spark.read.parquet(silver_spark_path)

    # 3. Compute Gold Layer Business Metrics (Aggregations)
    gold_df = (
        silver_df.groupBy("city")
        .agg(
            F.count("measurement_timestamp").alias("total_readings"),
            F.round(F.avg("temperature_c"), 2).alias("avg_temp_c"),
            F.max("temperature_c").alias("max_temp_c"),
            F.min("temperature_c").alias("min_temp_c"),
            F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
            F.max("wind_speed_kmh").alias("max_wind_speed_kmh"),
            F.max("ingestion_timestamp").alias("last_updated_at"),
        )
        .orderBy("city")
    )

    print("\n Gold Layer Schema:")
    gold_df.printSchema()

    print("\n Gold Layer Analytics Preview:")
    gold_df.show(truncate=False)

    # 4. Write to Gold Layer as Parquet
    gold_spark_path = str(gold_dir.as_posix())
    print(f"\n Writing Gold analytics dataset to: {gold_spark_path}")
    gold_df.write.mode("overwrite").parquet(gold_spark_path)

    print(" Successfully generated Gold Layer aggregations!")


if __name__ == "__main__":
    process_silver_to_gold()