# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read all the data as required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("number","driver_number").withColumnRenamed("name","driver_name").withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name","race_name").withColumnRenamed("race_stamp","race_date")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
.filter(f"file_date = '{v_file_date}'")\
.withColumnRenamed("time","race_time").withColumnRenamed("race_id", "result_race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join circuits to race

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id,races_df.year, races_df.race_name,circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join results to all other dataframe

# COMMAND ----------

race_results_df = results_df.join(race_circuit_df, results_df.result_race_id == race_circuit_df.race_id)\
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructorid)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id","year", "race_name","circuit_location", "driver_name", "driver_number", "driver_nationality","team","grid", "fastest_lap", "race_time", "points", "position").withColumn("created_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write outptu data to parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE f1_presentation.race_results;
# MAGIC

# COMMAND ----------

final_df.printSchema()


# COMMAND ----------

final_df = final_df.drop("race_id")


# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_time')


# COMMAND ----------

# Check the schema of final_df to confirm available columns
display(final_df)

# Update columns_to_write to include only existing columns
columns_to_write = [
    'year', 'race_name', 'circuit_location', 'driver_name', 'driver_number', 
    'driver_nationality', 'team', 'grid', 'fastest_lap', 'race_time', 
    'points', 'position', 'created_date', 'race_id'
]

final_df = final_df.select(columns_to_write)

overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')