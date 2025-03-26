# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake Containers for the Project
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    
    #get secrets from key vault

    client_id = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-service-principal-client-id")
    tenant_id = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-service-principal-tenant-id")
    client_secret = dbutils.secrets.get(scope="formula1car-scope", key="formula1car-service-principal-client-secret")

    #set spark configuration

    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if its already exists

    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    #mount the storage account container

    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())



# COMMAND ----------

# MAGIC %md
# MAGIC ### Monut Demo Container

# COMMAND ----------

mount_adls("formula1cardatalake", "demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monut Raw Container

# COMMAND ----------

mount_adls("formula1cardatalake", "raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monut Processed Container

# COMMAND ----------

mount_adls("formula1cardatalake", "processed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monut Presentation Container

# COMMAND ----------

mount_adls("formula1cardatalake", "presentation")