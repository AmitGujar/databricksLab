# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed('name', 'team')

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed('time', 'race_time')

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/race") \
    .withColumnRenamed('tournament', 'race_name') \
    .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

# MAGIC %md 
# MAGIC Join circuit and races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "date", "circuit_location", "driver_name", "driver_number",
"driver_nationality", "team", "grid", "fastest_lap", "race_time", "points") \
    .withColumn('created_date', current_timestamp())

# COMMAND ----------

# filtered_df = final_df.filter((final_df["race_year"] == 2020) & (final_df["race_name"] == "Abu Dhabi Grand Prix")) \
#     .sort(desc("points"))
# sorting the data in descending points order
filtered_df = final_df.filter((final_df["race_year"] == 2020))\
    .sort(desc("points"))

# COMMAND ----------

filtered_df.write.mode('overwrite') \
    .parquet(f"{presentation_folder_path}/race_results")

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------


