# Databricks notebook source
# MAGIC %md
# MAGIC ## Read CSV file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),StructField("driverId",IntegerType(),True),StructField("lap",IntegerType(),True),StructField("position",IntegerType(),True),StructField("time",StringType(),True),StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv("/mnt/formula1cardatalake/raw/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename columns and add the ingestion date**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the output to the parquet file**

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")