# Databricks notebook source
# MAGIC %run /Workspace/Users/jenitjain10@gmail.com/hackathon_cavallo/src/config

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Fix UDF registration error
from datetime import datetime, timedelta
 
def convert_nasa_decimal_to_utc(decimal_hours, year=2026):
    base_date = datetime(year, 1, 1)
 
    actual_utc = base_date + timedelta(hours=(decimal_hours - 24))
   
    return actual_utc.strftime("%Y-%m-%d %H:%M:%S")

convert_udf = F.udf(convert_nasa_decimal_to_utc)

# COMMAND ----------

# DBTITLE 1,Untitled
def process_batch(df, batch_id):
    # Perform pivot and aggregation on the current batch DataFrame
    df = (
        df
        .withColumn("value", F.col("value").cast("double"))
        .withColumn("timestamp", F.col("timestamp").cast("double"))
        .withColumn("timestamp", F.to_timestamp(convert_udf(F.col("timestamp"))))
    )
    
    pivoted_df = df.groupby("timestamp").pivot("item_name", field_names).agg(F.first("value"))

    pivoted_df.createOrReplaceTempView("pivoted_df_view")
    target_table_name = 'workspace.default.nasa_telemetry_silver'
    if spark.catalog.tableExists(target_table_name):
        target_table = spark.read.table(target_table_name)
        target_table.alias("target").merge(
            pivoted_df.alias("src"),
            "src.timestamp = target.timestamp AND src.item_name = target.item_name",
        ).whenNotMatchedInsertAll()
    else:
        pivoted_df.write.format("delta").mode("append").saveAsTable(target_table_name)
    
    # Write the result to your destination (e.g., a Delta table)
    

# Read from a streaming source
streaming_df = spark.readStream.table("workspace.default.nasa_telemetry_raw")

# Apply foreachBatch to process each micro-batch
query = (
    streaming_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/Volumes/workspace/default/nasa_data/checkpoints_silver/")
    .trigger(availableNow=True)
    .start()
)


# COMMAND ----------

query.awaitTermination()