# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").filter("circuit_id <=70").withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("year == 2019").withColumnRenamed("name", "race_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner Join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Outer join

# COMMAND ----------

#left outer join

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

#right outer join

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

#full outer join

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semi Joins

# COMMAND ----------

#Its similar to inner join but just disaplys left side of the join

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti join

# COMMAND ----------

#its opposite to semi

race_circuit_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross Join

# COMMAND ----------

race_circuit_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

int(races_df.count()) * int(circuits_df.count())