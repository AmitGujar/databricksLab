# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
#importing the functions

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
    .csv('/mnt/tfstorageisgreat53/raw/races.csv')
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
                                  .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
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

updated_race_data.write.mode('overwrite').parquet('/mnt/tfstorageisgreat53/processed/race')
# writing the updated data in parquet file format in adls

# COMMAND ----------

display(spark.read.parquet('/mnt/tfstorageisgreat53/processed/race'))
# displaying the data from parquet file which is processed in adls

# COMMAND ----------


