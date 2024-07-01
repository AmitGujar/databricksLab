# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# using databricks widgets
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
v_data_source


# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# using databricks widgets
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
v_data_source


# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

qualify_schema = StructType(fields=[
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

qualify_df = spark.read \
    .schema(qualify_schema) \
    .option('multiline', True) \
    .json(f'{raw_folder_path}/qualifying')

# COMMAND ----------

qualify_final = qualify_df.withColumnRenamed('qualifyId', 'qualify_id') \
                          .withColumnRenamed('raceId', 'race_id') \
                          .withColumnRenamed('driverId', 'driver_id') \
                          .withColumnRenamed('constructorId', 'constructor_id') \
                          .withColumn('data_source', lit(v_data_source))
qualify_final_df = add_ingestion_date(qualify_final)
# calling another function

# COMMAND ----------

qualify_final_df.write.mode('overwrite') \
    .parquet(f'{processed_folder_path}/qualifying')

display(spark.read.parquet(f'{processed_folder_path}/qualifying'))

# COMMAND ----------

dbutils.notebook.exit("Success")
