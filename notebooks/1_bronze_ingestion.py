# Databricks notebook source
dbutils.widgets.text("file_name","")

# COMMAND ----------

db_file_name= dbutils.widgets.get("file_name")

# COMMAND ----------

db_file_name

# COMMAND ----------

raw_path = f"s3://retail-datalake-de/raw/{db_file_name}"
bronze_path = f"s3://retail-datalake-de/bronze/{db_file_name}_bronze"
bronze_checkpoint = f"s3://retail-datalake-de/checkpoints_{db_file_name}/bronze/checkpoint/"
bronze_schema = f"s3://retail-datalake-de/checkpoints_{db_file_name}/bronze/schema/"

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("s3://retail-datalake-de/raw/sales")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading
# MAGIC

# COMMAND ----------

bronze_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")   # ✅ CSV because S3 has CSV
        .option("cloudFiles.schemaLocation", bronze_schema)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(raw_path)
)

# COMMAND ----------

display(
    df,
    checkpointLocation="s3://retail-datalake-de/checkpoints/sales/display_checkpoint/"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing

# COMMAND ----------

# # bronze_path = "s3://retail-datalake-de/bronze/sales_bronze"
# df.writeStream.format("csv")\
#     .outputMode("append")\
#     .option("checkpointLocation", f"s3://retail-datalake-de/checkpoints_{db_file_name}")\
#     .option("path", "s3://retail-datalake-de/bronze/sales_bronze")\
#     .trigger(once=True)\
#     .start()



# COMMAND ----------

(
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", bronze_checkpoint)
        .trigger(once=True)
        .start(bronze_path)
)


# COMMAND ----------

# df = spark.read.format("delta").load(bronze_path)
# display(df)