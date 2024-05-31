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
                                 StructField('time', TimestampType(), True),
                                 StructField('url', StringType(), True)   
])
# structypes will represent the rows and structfield will represent the columns
# defining the schema 

# COMMAND ----------

race_info=spark.read \
    .option('header', True) \
    .schema(race_schema) \
    .csv('/mnt/tfstorageisgreat53/raw/races.csv')
display(race_info)
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
display(race_data)
# filtering the columns except url

# COMMAND ----------

updated_race_data=race_data.withColumnRenamed('raceId', 'race_id') \
         .withColumnRenamed('circuitId', 'circuit_id') \
         .withColumnRenamed('name', 'tournament')
display(updated_race_data)
# updating the column headings

# COMMAND ----------

final_race_data=updated_race_data.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(''), col('time')), 'yyyy-MM-dd HH:mm:ss'))
display(final_race_data)
# adding the new column

# COMMAND ----------


