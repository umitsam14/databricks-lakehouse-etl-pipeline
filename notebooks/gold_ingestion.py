# Databricks notebook source
from pyspark.sql.functions import sum as _sum, count, avg

# ================================
# 0️⃣ Define dataset name and paths
# ================================
db_file_name = "sales"
silver_path = f"s3://retail-datalake-de/silver/{db_file_name}_silver"
gold_path = f"s3://retail-datalake-de/gold/{db_file_name}_gold"
gold_checkpoint = f"s3://retail-datalake-de/checkpoints_{db_file_name}/gold/checkpoint/"

# ================================
# 1️⃣ Clean old Gold / checkpoint (optional)
# ================================
dbutils.fs.rm(gold_path, True)
dbutils.fs.rm(gold_checkpoint, True)
dbutils.fs.mkdirs(gold_checkpoint)

# ================================
# 2️⃣ Read Silver as streaming Delta
# ================================
silver_stream_df = spark.readStream.format("delta").load(silver_path)

# ================================
# 3️⃣ Aggregate for Gold (per customer)
# ================================
gold_stream_df = (
    silver_stream_df
    .groupBy("customer_id")
    .agg(
        _sum("total_cost").alias("total_spent"),
        count("order_id").alias("orders_count"),
        avg("total_cost").alias("avg_order_value")
    )
)

# ================================
# 4️⃣ Write Gold as streaming Delta
# ================================
gold_query = (
    gold_stream_df.writeStream
        .format("delta")
        .outputMode("complete")  # required for aggregation
        .option("checkpointLocation", gold_checkpoint)
        .trigger(availableNow=True)  # process all current data
        .start(gold_path)
)

gold_query.awaitTermination()

# ================================
# 5️⃣ Verify Gold table (batch read)
# ================================
gold_df = spark.read.format("delta").load(gold_path)
display(gold_df)

# ================================
# 6️⃣ Register Gold table in catalog
# ================================
spark.sql("CREATE DATABASE IF NOT EXISTS Databricks_cat_project")

# Use a new table name to avoid LOCATION_OVERLAP
spark.sql(f"""
CREATE TABLE IF NOT EXISTS Databricks_cat_project.gold_table_new
USING DELTA
LOCATION '{gold_path}'
""")
