# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import to_timestamp, col

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("item_name", StringType()),
    StructField("value", StringType())
])

raw_df = (spark.readStream
    .schema(schema)           # avoid inference for stability
    .json(RAW_DIR)            # tail NDJSON files
)

# Write to Delta (append) with checkpointing
query = (raw_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(availableNow=True)
    .table("workspace.default.nasa_telemetry_raw")
)

# COMMAND ----------

spark.read.table("workspace.default.nasa_telemetry_raw").display()