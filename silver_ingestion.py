# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

dbutils.widgets.text("file_name","")
db_file_name = dbutils.widgets.get("file_name")


# COMMAND ----------

dbutils.fs.ls("s3://retail-datalake-de/raw/")


# COMMAND ----------

bronze_path = f"s3://retail-datalake-de/bronze/{db_file_name}_bronze"

silver_path = f"s3://retail-datalake-de/silver/{db_file_name}_silver"
silver_checkpoint = f"s3://retail-datalake-de/checkpoints_{db_file_name}/silver/checkpoint/"
silver_schema = f"s3://retail-datalake-de/checkpoints_{db_file_name}/silver/schema/"

# COMMAND ----------

# ================================
# 3️⃣ Ensure Bronze exists (create from CSV/JSON if missing)
# ================================
try:
    dbutils.fs.ls(bronze_path)
    print(f"Bronze path exists: {bronze_path}")
except:
    print(f"Bronze path NOT found. Creating Bronze table from raw data...")

    # Example: Load raw CSV from S3 (replace path with your raw data location)
    raw_csv_path = f"s3://retail-datalake-de/raw/{db_file_name}.csv"

    # Read CSV
    bronze_df = spark.read.format("csv").option("header","true").load(raw_csv_path)

    # Write as Delta
    bronze_df.write.format("delta").mode("overwrite").save(bronze_path)
    print(f"Bronze Delta table created at {bronze_path}")

# COMMAND ----------

dbutils.fs.rm(silver_path, True)
dbutils.fs.rm(silver_checkpoint, True)

# COMMAND ----------

dbutils.fs.mkdirs(silver_checkpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Read

# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_path)


# COMMAND ----------

dbutils.fs.ls("s3://retail-datalake-de/bronze/sales_bronze")


# COMMAND ----------

silver_df = (
    bronze_df
    .withColumn("quantity", col("quantity").cast(DoubleType()))
    .withColumn("price", col("price").cast(DoubleType()))
    .withColumn("total_cost", col("quantity") * col("price"))
    .filter(col("order_status").isNotNull())
    .dropDuplicates(["order_id"])
)

# COMMAND ----------

display(silver_df)


# COMMAND ----------

silver_df.limit(0).write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

silver_query = (
    silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", silver_checkpoint)
        .trigger(availableNow=True)   # process all current files
        .start(silver_path)
)

# COMMAND ----------

silver_query.awaitTermination()


# COMMAND ----------

df = spark.read.format("delta").load(silver_path)
display(df)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS Databricks_cat_project")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS Databricks_cat_project.silver_table
USING DELTA
LOCATION '{silver_path}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

# silver_df = (
#     spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "delta")   # reading Bronze Delta
#         .option("cloudFiles.schemaLocation", silver_schema)
#         .load(bronze_path)
# )

# silver_df = (
#     spark.readStream
#         .format("delta")     # ✅ native Delta streaming
#         .load(bronze_path)
# )

# COMMAND ----------

# silver_df = spark.read.format("delta").load(bronze_path)
# display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing

# COMMAND ----------

# silver_schema_df = (
#     bronze_df
#     .withColumn("quantity", col("quantity").cast(DoubleType()))
#     .withColumn("price", col("price").cast(DoubleType()))
#     .withColumn("total_cost", col("quantity") * col("price"))
#     .filter(col("order_status").isNotNull())
#     .dropDuplicates(["order_id"])
# )

# COMMAND ----------

# silver_schema_df.write.format("delta").mode("overwrite").save(silver_path)
# dbutils.fs.mkdirs(silver_checkpoint)

# COMMAND ----------

# (
#     silver_df.writeStream
#         .format("delta")
#         .outputMode("append")
#         .option("checkpointLocation", silver_checkpoint)
#         .trigger(availableNow=True)   # ✅ serverless compatible
#         .start(silver_path)
# )

# COMMAND ----------

# df = spark.read.format("delta").load(silver_path)
# display(df)

# COMMAND ----------

# from pyspark.sql.types import DoubleType
# silver_df = (

#     spark.readStream
#         .format("delta")
#         .load(bronze_path)
#         .withColumn("quantity", col("quantity").cast(DoubleType()))
#         .withColumn("price", col("price").cast(DoubleType()))
#         .withColumn("total_cost", col("quantity") * col("price"))
#         .filter(col("order_status").isNotNull())
#         .dropDuplicates(["order_id"])
# )