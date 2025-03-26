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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True), StructField("surname", StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False), StructField("driverRef", StringType(), True), StructField("number", IntegerType(), True), StructField("code", StringType(), True), StructField("name", name_schema, True), StructField("dob", DateType(), True), StructField("nationality", StringType(), True), StructField("url", StringType(), True)])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename columns and add the ingestion date**

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_coulumns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                     .withColumnRenamed("driverRef", "driver_ref")\
                                     .withColumn("ingestion_date", current_timestamp())\
                                     .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                     .withColumn("file_date", lit(v_file_date))     #creating a new column

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop the unwanted columns**

# COMMAND ----------

drivers_final_df = drivers_with_coulumns_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the output to the parquet file**

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")