# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="formula1car-scope")

# COMMAND ----------

dbutils.secrets.get(scope="formula1car-scope", key="formula1car-account-key")

# COMMAND ----------

