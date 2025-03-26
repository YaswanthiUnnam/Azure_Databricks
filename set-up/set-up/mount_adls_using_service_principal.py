# Databricks notebook source
dbutils.secrets.list(scope="formula1car-scope")

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-service-principal-client-id")
tenant_id = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-service-principal-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-service-principal-client-secret")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1cardatalake.dfs.core.windows.net/",
  mount_point = "/mnt/formula1cardatalake/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1cardatalake/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1cardatalake/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

