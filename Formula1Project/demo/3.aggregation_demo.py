# Databricks notebook source
# MAGIC %md
# MAGIC Aggregate functions

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter('race_year=2020')
display(race_results_df)

# COMMAND ----------

demo_df.select(count('race_name')).show()
# this will return number of records in df

# COMMAND ----------

demo_df.select(countDistinct('race_name')).show()
# this will return number of records in df different race_name

# COMMAND ----------

demo_df.select(sum('points')).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points')).show()
# using sum with filter function

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points'), countDistinct('race_name')) \
    .withColumnRenamed('sum(points)', 'total_points') \
    .withColumnRenamed('count(DISTINCT race_name)', 'number_of_races') \
    .show()
# using sum with filter function

# COMMAND ----------

demo_df.groupBy('driver_name').sum('points').show()
#we can't use countdistinct on dataframe so we are using agg

# COMMAND ----------

demo_df.groupBy('driver_name') \
       .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races')) \
       .show()
#we can't use countdistinct on dataframe so we are using agg for use it as aggregate functions
#we are using alias to change column names on temp basis

# COMMAND ----------


