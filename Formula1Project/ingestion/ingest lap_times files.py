# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

pitstops_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('stop', IntegerType(), True),
    StructField('lap', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

pitstop_df = spark.read \
    .schema(pitstops_schema) \
    .option('multiline', True) \
    .json('/mnt/tfstorageisgreat13/raw/pit_stops.json')
#use multiline is true if you are reading multiline json file

# COMMAND ----------

pitstop_final=pitstop_df.withColumnRenamed('raceId', 'race_id') \
                        .withColumnRenamed('driverId', 'driver_id') \
                        .withColumn('ingestion_date', current_timestamp())
# in this we are renaming and adding ingestion date column with current timestamp


# COMMAND ----------

pitstop_final.write.mode('overwrite') \
    .parquet('/mnt/tfstorageisgreat13/processed/pitstops')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tfstorageisgreat13/processed

# COMMAND ----------

display(spark.read.parquet('/mnt/tfstorageisgreat13/processed/pitstops'))

# COMMAND ----------


