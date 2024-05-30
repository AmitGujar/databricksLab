# Databricks notebook source
client_id="246cfd43-77c5-4c63-8f77-d6a19a3876ec"
tenant_id="e4e34038-ea1f-4882-b6e8-ccd776459ca0"
secret="u9Z8Q~8Y-Lqc6nj52HHI9qUBlL4D4CXqi_5zta8t"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.tfstorageisgreat53.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.tfstorageisgreat53.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.tfstorageisgreat53.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.tfstorageisgreat53.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.tfstorageisgreat53.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

storageContents=dbutils.fs.ls("abfss://test@tfstorageisgreat53.dfs.core.windows.net")
display(storageContents)

# COMMAND ----------

table=spark.read.csv("abfss://test@tfstorageisgreat53.dfs.core.windows.net/circuits.csv")
display(table)

# COMMAND ----------


