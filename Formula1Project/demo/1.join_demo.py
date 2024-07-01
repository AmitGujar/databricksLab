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
circuits_df_raw = spark.read.parquet(f'{processed_folder_path}/circuits')
circuits_df = circuits_df_raw.filter(circuits_df_raw['circuit_id'] < 70)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(circuits_df.name, circuits_df.location, circuits_df.country, races_df.tournament, races_df.round)
# inner join

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select('name').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Outer Join
# MAGIC

# COMMAND ----------

# Left Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
    .select(circuits_df.name, circuits_df.location, circuits_df.country, races_df.tournament, races_df.round)
display(race_circuits_df)
# left outer join

# COMMAND ----------

# right outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
    .select(circuits_df.name, circuits_df.location, circuits_df.country, races_df.tournament, races_df.round)
display(race_circuits_df)

# COMMAND ----------

# full outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
    .select(circuits_df.name, circuits_df.location, circuits_df.country, races_df.tournament, races_df.round)
display(race_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Semi Joins

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
    .select(circuits_df.name, circuits_df.location, circuits_df.country)
# works like inner join but we get columns from left data frame only
display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Anti Join

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti") 
display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cross Join

# COMMAND ----------

# cross join gives cartisiean product
race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.count()

# COMMAND ----------

int(races_df.count()) * int(circuits_df.count())
# this count is same as number of rows returned by the cross join

# COMMAND ----------


