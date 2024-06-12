# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# using databricks widgets
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
v_data_source


# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df=spark.read \
    .schema(constructors_schema) \
    .json("/mnt/tfstorageisgreat13/raw/constructors.json")

constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop unwanted columns from the dataframe

# COMMAND ----------

new_constructors_df=constructors_df.drop("url")
# or use can use below method to drop columns if you have multiple dataframes
new_constructors_df=constructors_df.drop(constructors_df['url'])
# use this if you have imported col function
new_constructors_df=constructors_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md 
# MAGIC rename and add ingestion date

# COMMAND ----------

constructor_final=new_constructors_df.withColumnRenamed('constructorId', 'constructor_id') \
                                     .withColumnRenamed('constructorRef', 'constructor_ref') \
                                     .withColumn('ingestion_date', current_timestamp()) \
                                     .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to parquet file

# COMMAND ----------

constructor_final.write.mode('overwrite').parquet("/mnt/tfstorageisgreat13/processed/constructors")

# COMMAND ----------

display(spark.read.parquet('/mnt/tfstorageisgreat13/processed/constructors'))

# COMMAND ----------

dbutils.notebook.exit('Success')
