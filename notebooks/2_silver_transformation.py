# Databricks notebook source
# ================================
# 1️⃣ Imports
# ================================
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

# ================================
# 2️⃣ Paths
# ================================
db_file_name = "sales"  # your dataset name
bronze_path = f"s3://retail-datalake-de/bronze/{db_file_name}_bronze"
silver_path = f"s3://retail-datalake-de/silver/{db_file_name}_silver"
silver_checkpoint = f"s3://retail-datalake-de/checkpoints_{db_file_name}/silver/checkpoint/"

# ================================
# 3️⃣ Clean old Silver / checkpoint (optional)
# ================================
dbutils.fs.rm(silver_path, True)
dbutils.fs.rm(silver_checkpoint, True)
dbutils.fs.mkdirs(silver_checkpoint)

# ================================
# 4️⃣ Read Bronze as streaming Delta
# ================================
bronze_stream_df = spark.readStream.format("delta").load(bronze_path)

# ================================
# 5️⃣ Transform Bronze → Silver
# ================================
silver_stream_df = (
    bronze_stream_df
    .withColumn("quantity", col("quantity").cast(DoubleType()))
    .withColumn("price", col("price").cast(DoubleType()))
    .withColumn("total_cost", col("quantity") * col("price"))
    .filter(col("order_status").isNotNull())
    .dropDuplicates(["order_id"])
)

# ================================
# 6️⃣ Start streaming write to Silver
# ================================
silver_query = (
    silver_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", silver_checkpoint)
        .trigger(availableNow=True)  # process all current files
        .start(silver_path)
)

# Wait for the stream to finish processing current files
silver_query.awaitTermination()

# ================================
# 7️⃣ Verify Silver table (batch read)
# ================================
silver_df = spark.read.format("delta").load(silver_path)
display(silver_df)

# ================================
# 8️⃣ Register Silver table in catalog
# ================================
spark.sql("CREATE DATABASE IF NOT EXISTS Databricks_cat_project")




# COMMAND ----------

# %sql
# SELECT * FROM delta.`s3://retail-datalake-de/silver/sales_silver`

silver_df = spark.read.format("delta").load("s3://retail-datalake-de/silver/sales_silver")
display(silver_df)

