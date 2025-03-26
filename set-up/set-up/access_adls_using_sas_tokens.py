# Databricks notebook source
dbutils.secrets.listScopes()


# COMMAND ----------

dbutils.secrets.list(scope="formula1car-scope")

# COMMAND ----------

formula1car_sas_token = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1cardatalake.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1cardatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1cardatalake.dfs.core.windows.net", formula1car_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1cardatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1cardatalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

