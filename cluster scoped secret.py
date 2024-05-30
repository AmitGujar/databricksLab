# Databricks notebook source
storageContents=dbutils.fs.ls("abfss://test@tfstorageisgreat53.dfs.core.windows.net")
display(storageContents)
# cluster scope is defined in spark config.

# COMMAND ----------

 
