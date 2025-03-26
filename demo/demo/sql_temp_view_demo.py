# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframe using SQL

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Local temporary view

# COMMAND ----------

#creating temporary view

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results
# MAGIC where year = 2019

# COMMAND ----------

# Passing parameters to SQL

p_year = 2017

# COMMAND ----------

# execute SQL from Python

race_results_2017_df = spark.sql(f"SELECT * FROM v_race_results where year = {p_year}")

# COMMAND ----------

display(race_results_2017_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temporary view

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()

# COMMAND ----------

