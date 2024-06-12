# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

docker_schema = StructType(fields=[
    StructField('container_id', StringType(), True)
])

# COMMAND ----------

kubernetes_schema = StructType(fields=[
    StructField('container_name', StringType(), True),
    StructField('namespace_name', StringType(), True),
    StructField('pod_name', StringType(), True),
    StructField('container_image', StringType(), True),
    StructField('container_image_id', StringType(), True),
    StructField('pod_id', StringType(), True),
    StructField('host', StringType(), True)
])

# COMMAND ----------

log_schema = StructType(fields=[
    StructField('stream', StringType(), True),
    StructField('character', StringType(), True),
    StructField('message', StringType(), True),
    StructField('docker', docker_schema, True),
    StructField('kubernetes', kubernetes_schema, True)
])

# COMMAND ----------

log_df = spark.read \
              .schema(log_schema) \
              .json(f'{raw_folder_path}/kafka_streams.json')

# COMMAND ----------

log_df.printSchema()

# COMMAND ----------

log_raw = log_df.withColumn('docker', col('docker.container_id')) \
                  .withColumn('container_name', col('kubernetes.container_name')) \
                  .withColumn('namespace_name', col('kubernetes.namespace_name')) \
                  .withColumn('pod_name', col('kubernetes.pod_name')) \
                  .withColumn('image', col('kubernetes.container_image')) \
                  .withColumn('image_id', col('kubernetes.container_image_id')) \
                  .withColumn('pod_id', col('kubernetes.pod_id')) \
                  .withColumn('node', col('kubernetes.host'))


# COMMAND ----------

log_final = log_raw.drop(col('kubernetes')) \
                   .drop(col('image')) \
                   .drop(col('image_id')) \
                   .drop(col('pod_id')) \
                   .drop(col('docker')) \
                   .drop(col('stream')) \
                   .drop(col('character'))

# COMMAND ----------

log_final.write.mode('overwrite') \
               .partitionBy('namespace_name') \
               .parquet(f'{processed_folder_path}/kubernetes_logs')

log_reader = spark.read.parquet(f'{processed_folder_path}/kubernetes_logs')
display(log_reader)

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %md
# MAGIC Using filters

# COMMAND ----------

log_filtered = log_final.filter(log_final["namespace_name"] == "elastic-deployment")
display(log_filtered)

# COMMAND ----------


