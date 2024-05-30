# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.tfstorageisgreat53.dfs.core.windows.net",
    dbutils.secrets.get(scope="key-scope", key="account-key"))

# COMMAND ----------

storageContents=dbutils.fs.ls("abfss://test@tfstorageisgreat53.dfs.core.windows.net")
display(storageContents)

# COMMAND ----------

table=spark.read.csv("abfss://test@tfstorageisgreat53.dfs.core.windows.net/circuits.csv")


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="sas-token-scope", key="sas-token"))

# COMMAND ----------

storageContents=dbutils.fs.ls("abfss://test@tfstorageisgreat53.dfs.core.windows.net")
display(storageContents)

# COMMAND ----------


