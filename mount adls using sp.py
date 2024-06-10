# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "246cfd43-77c5-4c63-8f77-d6a19a3876ec",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="sp-secret",key="client-secret"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/e4e34038-ea1f-4882-b6e8-ccd776459ca0/oauth2/token"}

# COMMAND ----------

def adlsMount(containerName, storageAccount):
  if any(mount.mountPoint == f"/mnt/{storageAccount}/{containerName}" for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(f"/mnt/{storageAccount}/{containerName}")

  dbutils.fs.mount(
      source = f"abfss://{containerName}@{storageAccount}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storageAccount}/{containerName}",
    extra_configs = configs)
  
  listMounts=dbutils.fs.mounts
  display(listMounts)

adlsMount('test', 'tfstorageisgreat13')

# COMMAND ----------

storageContents=dbutils.fs.ls("/mnt/tfstorageisgreat13/test")
display(storageContents)

# COMMAND ----------

dataRead=spark.read.csv('/mnt/tfstorageisgreat13/test/circuits.csv')
display(dataRead)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/tfstorageisgreat53/test')

# COMMAND ----------

adlsMount('presentation', 'tfstorageisgreat13')
adlsMount('processed', 'tfstorageisgreat13')
adlsMount('raw', 'tfstorageisgreat13')

# COMMAND ----------

# MAGIC %sh 
# MAGIC ps aux

# COMMAND ----------


