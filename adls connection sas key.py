# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.tfstorageisgreat14.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.tfstorageisgreat14.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.tfstorageisgreat14.dfs.core.windows.net", "sp=rl&st=2024-05-29T06:55:31Z&se=2024-06-07T14:55:31Z&spr=https&sv=2022-11-02&sr=c&sig=N0nKv%2FsS87t43uKtRJ%2BnVmE8JfFJef2XqwbvO%2ByLAbY%3D")

# COMMAND ----------

storageContents=dbutils.fs.ls("abfss://test@tfstorageisgreat14.dfs.core.windows.net")
display(storageContents)

# COMMAND ----------

table=spark.read.csv("abfss://test@tfstorageisgreat14.dfs.core.windows.net/circuits.csv")
display(table)

# COMMAND ----------


