# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce driver standing

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df\
    .groupBy('year','driver_name', 'driver_nationality','team')\
    .agg(sum('points').alias('total_points'), count(when(col('position') == '1', True)).alias('wins'))

# COMMAND ----------

display(driver_standings_df.filter(driver_standings_df.year == 2020))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc, asc, rank

driver_rank_spec = Window.partitionBy('year').orderBy(desc('total_points'), asc('wins'))
final_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter('year == 2020'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output data to parquet

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_presentation.driver_standings")