# Databricks notebook source
dbutils.secrets.list(scope="formula1car-scope")

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-service-principal-client-id")
tenant_id = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-service-principal-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-service-principal-client-secret")


# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formula1cardatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1cardatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1cardatalake.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1cardatalake.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1cardatalake.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1cardatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1cardatalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

