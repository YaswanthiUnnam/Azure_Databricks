# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce constructor standing

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df\
    .groupBy('year','team')\
    .agg(sum('points').alias('total_points'), count(when(col('position') == '1', True)).alias('wins'))

# COMMAND ----------

display(constructor_standings_df.filter(constructor_standings_df.year == 2020))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc, asc, rank

constructor_rank_spec = Window.partitionBy('year').orderBy(desc('total_points'), asc('wins'))
final_df = constructor_standings_df.withColumn('rank', rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter('year == 2020'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output data to parquet

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

