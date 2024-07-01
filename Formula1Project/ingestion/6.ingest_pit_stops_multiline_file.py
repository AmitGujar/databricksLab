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

lap_times_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('lap', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

laptimes_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f'{raw_folder_path}/lap_times')
#    .csv('/mnt/tfstorageisgreat13/raw/lap_times/*.csv') this is another method 
#use multiline is true if you are reading multiline json file

# COMMAND ----------

laptimes_final=laptimes_df.withColumnRenamed('raceId', 'race_id') \
                        .withColumnRenamed('driverId', 'driver_id') \
                        .withColumn('ingestion_date', current_timestamp()) \
                        .withColumn('data_source', lit(v_data_source))
# in this we are renaming and adding ingestion date column with current timestamp


# COMMAND ----------

laptimes_final.write.mode('overwrite') \
    .parquet(f'{processed_folder_path}/laptimes')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tfstorageisgreat16/processed

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/laptimes'))

# COMMAND ----------

dbutils.notebook.exit('Success')
