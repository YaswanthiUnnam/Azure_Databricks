# Databricks notebook source
# MAGIC %md
# MAGIC ## Read JSON file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False), StructField("raceId", IntegerType(), True), StructField("driverId", IntegerType(), True), StructField("constructorId", IntegerType(), True), StructField("number", IntegerType(), True), StructField("grid", IntegerType(), True), StructField("position", StringType(), True), StructField("positionText", StringType(), True), StructField("positionOrder", IntegerType(), True), StructField("points", FloatType(), True), StructField("laps", IntegerType(), True), StructField("time", StringType(), True), StructField("milliseconds", IntegerType(), True), StructField("fastestLap", IntegerType(), True), StructField("rank", IntegerType(), True), StructField("fastestLapTime", StringType(), True), StructField("fastestLapSpeed", FloatType(), True), StructField("statusId", IntegerType(), True)])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename columns and add the ingestion date**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_column_df = results_df.withColumnRenamed("resultId","result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder","position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumnRenamed("statusId","status_id").withColumn("ingetion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop the unwanted columns**

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_column_df.drop(col("status_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the output to the parquet file**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Load Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect(): #collect takes all the data and puts in driver memoery
#  if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):   
#   spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION(race_id={race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format('parquet').saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Load Method 2

# COMMAND ----------

output_df = rearrange_partition_column(results_final_df, "race_id")

# COMMAND ----------

results_final_df = results_final_df.withColumnRenamed("ingestion_date", "ingetion_date")

# COMMAND ----------

from pyspark.sql.functions import col

# Explicitly cast to match the table schema
results_final_df = results_final_df.withColumn("ingetion_date", col("ingetion_date").cast("string"))
results_final_df = results_final_df.withColumn("ingetion_date", col("ingetion_date").cast("timestamp"))


# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.results;

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingetion_date", "race_id")

# COMMAND ----------

overwrite_partition(results_final_df, "f1_processed","results", "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc;