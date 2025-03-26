# Databricks notebook source
formula1car_account_key = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-account-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1cardatalake.dfs.core.windows.net",
    formula1car_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1cardatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1cardatalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

