# Databricks notebook source
# MAGIC %md
# MAGIC ## Read JSON file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pits_stop_schema = StructType(fields=[StructField("raceId",IntegerType(),False),StructField("driverId",IntegerType(),True),StructField("stop",StringType(),True),StructField("lap",IntegerType(),True),StructField("time",StringType(),True),StructField("duration",StringType(),True), StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

pits_stop_df = spark.read.schema(pits_stop_schema).option("multiLine",True).json("/mnt/formula1cardatalake/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename columns and add the ingestion date**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pits_stop_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the output to the parquet file**

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")