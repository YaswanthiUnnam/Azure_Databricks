# Databricks notebook source
# MAGIC %md
# MAGIC ## Read CSV file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True)])


# COMMAND ----------

circuits_df = spark.read.option("header", "true")\
    .schema(circuits_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only the required Columns

# COMMAND ----------

# MAGIC %md
# MAGIC **4 Different ways to select columns**

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col;

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion date to the dataframe

# COMMAND ----------

cicuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet

# COMMAND ----------

cicuits_final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")