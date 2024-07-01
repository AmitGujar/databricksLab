# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/race")

# COMMAND ----------

races_filtered_df = races_df.filter('race_year = 2019 and round <= 5')
# sql way to specify multiple conditions
races_filtered_new = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <=5))
# python way to specify multiple conditions

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------


