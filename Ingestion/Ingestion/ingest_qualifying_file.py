# Databricks notebook source
# MAGIC %md
# MAGIC ## Read JSON file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False), StructField("raceId", IntegerType(), True), StructField("driverId", IntegerType(), True),StructField("constructorId", IntegerType(), True), StructField("number", IntegerType(), True), StructField("position", IntegerType(), True), StructField("q1", StringType(), True), StructField("q2", StringType(), True), StructField("q3", StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json("/mnt/formula1cardatalake/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename columns and add the ingestion date**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the output to the parquet file**

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")