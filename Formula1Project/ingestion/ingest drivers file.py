# Databricks notebook source
# MAGIC %md
# MAGIC Read json file

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

name_schema = StructType(fields=[
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema, True),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json('/mnt/tfstorageisgreat13/raw/drivers.json')
drivers_df.printSchema()

# COMMAND ----------

drivers_final = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                          .withColumnRenamed('driverRef', 'driver_ref') \
                          .withColumn('ingestion_date', current_timestamp()) \
                          .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))

# COMMAND ----------

drivers_final_df = drivers_final.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC export in parquet format
# MAGIC

# COMMAND ----------

drivers_final_df.write.mode('overwrite') \
    .parquet('/mnt/tfstorageisgreat13/processed/drivers')
display(spark.read.parquet('/mnt/tfstorageisgreat13/processed/drivers'))

# COMMAND ----------


