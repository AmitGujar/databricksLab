# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# reading race file
races_df_raw = spark.read.parquet(f'{processed_folder_path}/race')
races_df = races_df_raw.filter(races_df_raw['race_year'] == 2019)

# COMMAND ----------

# reading circuits file
circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(circuits_df.name, circuits_df.location, circuits_df.country, races_df.tournament, races_df.round)
# inner join

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select('name').show()

# COMMAND ----------


