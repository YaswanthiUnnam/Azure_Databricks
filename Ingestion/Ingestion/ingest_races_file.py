# Databricks notebook source
# MAGIC %md
# MAGIC ## Read CSV file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False), StructField("year", IntegerType(), True), StructField("round", IntegerType(), True), StructField("circuitId", IntegerType(), True), StructField("name", StringType(), True), StructField("date", DateType(), True), StructField("time", StringType(), True), StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read.option("header", "true")\
    .schema(race_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

type(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only the required Columns

# COMMAND ----------

from pyspark.sql.functions import col;

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"), col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, col, lit, concat

# COMMAND ----------


races_final_df = races_renamed_df.withColumn("ingestion_date", current_timestamp())\
                                 .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("year").format('parquet').saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")