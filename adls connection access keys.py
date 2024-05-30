# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.tfstorageisgreat53.dfs.core.windows.net",
    "vEwGH82GbVdsk+rvnFzSYJQTmsV2RFK3dpkJC28rMmAFNnK4iM/YBCNUxXEcawnVXSDKS7yck6U5+AStOamecw=="
)

# COMMAND ----------

storageContents=dbutils.fs.ls("abfss://test@tfstorageisgreat53.dfs.core.windows.net")
display(storageContents)

# COMMAND ----------

table=spark.read.csv("abfss://test@tfstorageisgreat53.dfs.core.windows.net/circuits.csv")
display(table)

# COMMAND ----------


