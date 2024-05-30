# Databricks notebook source
files=dbutils.fs.ls('/')
display(files)

# COMMAND ----------

listFiles=dbutils.fs.ls('/FileStore')
display(listFiles)

# COMMAND ----------

listData=spark.read.csv('dbfs:/FileStore/circuits.csv')
display(listData)

# COMMAND ----------


