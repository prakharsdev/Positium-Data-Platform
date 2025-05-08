import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, min, max, explode, array, struct

# Set PySpark to use your current Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 4g --executor-memory 4g pyspark-shell"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Compress Mobile Events") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Define paths
input_path = "data/raw/events/year=2024/month=10/day=1/"
output_path = "data/processed/compressed_events/"

print(f"Reading raw events from: {input_path}")
df = spark.read.parquet(input_path)

# Add hour column
df = df.withColumn("hour", hour("timestamp"))

# Compress: Keep only first and last timestamp per user_id, cell_id, hour
compressed = df.groupBy("user_id", "cell_id", "hour") \
    .agg(
        min("timestamp").alias("first_seen"),
        max("timestamp").alias("last_seen")
    )

# Expand into two rows per group: first and last
expanded = compressed.selectExpr(
    "user_id",
    "cell_id",
    "explode(array(struct(first_seen as timestamp), struct(last_seen as timestamp))) as event"
).select("user_id", "cell_id", "event.timestamp")

# Write compressed data
print(f"Writing compressed events to: {output_path}")
expanded.write.mode("overwrite").parquet(output_path, compression="snappy")

print("Event compression complete.")
spark.stop()
