# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregations function demo

# COMMAND ----------

# MAGIC %md
# MAGIC **Built-in aggregate functions**

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(count("year")).show()

# COMMAND ----------

demo_df.select(countDistinct("year")).show()

# COMMAND ----------

demo_df.select(sum("year")).show()

# COMMAND ----------

demo_df.filter("driver_name ='Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name ='Lewis Hamilton'").select(sum("points"), countDistinct("race_name"))\
    .withColumnRenamed("sum(points)", "total_points")\
    .withColumnRenamed("count(DISTINCT race_name)", "number_of_races").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group Aggregations

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

demo_df = race_results_df.filter(race_results_df.year.isin(2019, 2020))

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("year","driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show()

# COMMAND ----------

