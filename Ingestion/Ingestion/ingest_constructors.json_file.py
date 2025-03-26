# Databricks notebook source
# MAGIC %md
# MAGIC ## Read JSON file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING "

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename columns and add ingestion date**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructorid").withColumnRenamed("constructorRef", "constructorref").withColumn("ingestiondate", current_timestamp()).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC **Write output to parquet file**

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")