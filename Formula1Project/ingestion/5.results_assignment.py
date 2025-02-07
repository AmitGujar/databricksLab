# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# using databricks widgets
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
v_data_source


# COMMAND ----------

result_schema = StructType(fields=[
    StructField('resultId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), True),
    StructField('positionOrder', IntegerType(), True),
    StructField('points', FloatType(), True),
    StructField('laps', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('fastestLap', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('fastestLapTime', StringType(), True),
    StructField('fastestLapSpeed', StringType(), True),
    StructField('statusId', IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read \
    .schema(result_schema) \
    .json(f'{raw_folder_path}/results.json')

results_df.printSchema()

# COMMAND ----------

result_renamed=results_df.withColumnRenamed('resultId', 'result_id') \
                         .withColumnRenamed('raceId', 'race_id') \
                         .withColumnRenamed('driverId', 'driver_id') \
                         .withColumnRenamed('constructorId', 'constructor_id') \
                         .withColumnRenamed('positionText', 'position_text') \
                         .withColumnRenamed('positionOrder', 'position_order') \
                         .withColumnRenamed('fastestLap', 'fastest_lap') \
                         .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                         .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')

# COMMAND ----------

results_dropped=result_renamed.drop(col('statusId'))

# COMMAND ----------

result_final=results_dropped.withColumn('ingestion_date', current_timestamp()) \
                            .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

result_final.write.mode('overwrite') \
    .partitionBy('race_id') \
    .parquet(f'{processed_folder_path}/results')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/results'))

# COMMAND ----------

dbutils.notebook.exit('Success')
