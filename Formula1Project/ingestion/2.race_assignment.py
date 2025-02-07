# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
#importing the functions

# COMMAND ----------

# using databricks widgets
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
v_data_source


# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField('year', IntegerType(), True),
                                 StructField('round', IntegerType(), True),
                                 StructField('circuitId', IntegerType(), True),
                                 StructField('name', StringType(), True),
                                 StructField('date', DateType(), True),
                                 StructField('time', StringType(), True),
                                 StructField('url', StringType(), True)   
])
# structypes will represent the rows and structfield will represent the columns
# defining the schema 

# COMMAND ----------

race_info=spark.read \
    .option('header', True) \
    .schema(race_schema) \
    .csv(f'{raw_folder_path}/races.csv')
# readting the file as per the schema

# COMMAND ----------

race_info.printSchema()
# validating the schema

# COMMAND ----------

race_data=race_info.select(col('raceId'),
                           col('year'),
                           col('round'),
                           col('circuitId'),
                           col('name'),
                           col('date'),
                           col('time'))
# filtering the columns except url

# COMMAND ----------

updated_race_data=race_data.withColumnRenamed('raceId', 'race_id') \
         .withColumnRenamed('circuitId', 'circuit_id') \
         .withColumnRenamed('year', 'race_year') \
         .withColumnRenamed('name', 'tournament')
# updating the column headings

# COMMAND ----------

race_with_time = updated_race_data.withColumn('ingestion_date', current_timestamp()) \
                                  .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                  .withColumn('data_source', lit(v_data_source))
# adding the new column

# COMMAND ----------

final_race_data=race_with_time.select(col('race_id'),
                                      col('race_year'),
                                      col('round'),
                                      col('circuit_id'),
                                      col('tournament'),
                                      col('ingestion_date'),
                                      col('race_timestamp'))
# filtering furthur colums from the table

# COMMAND ----------

updated_race_data.write.mode('overwrite') \
                       .partitionBy('race_year') \
                       .parquet(f'{processed_folder_path}/race')
# writing the updated data in parquet file format in adls
# we can partition data in folders with the partitionBy method

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/race'))
# displaying the data from parquet file which is processed in adls

# COMMAND ----------

dbutils.notebook.exit("Success")
