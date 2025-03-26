# Databricks notebook source
v_result = dbutils.notebook.run("ingest_circuits_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_constructors.json_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_drivers_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_lap_times_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_pitstops_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_qualifying_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_races_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_results_file", 0, {"p_data_source": "Ergast API"})