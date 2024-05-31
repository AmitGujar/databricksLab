# Databricks notebook source
# MAGIC %md
# MAGIC Read the csv file using spark data frame reader

# COMMAND ----------

fileMounts=dbutils.fs.mounts()
display(fileMounts)

# COMMAND ----------

result=spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/tfstorageisgreat53/raw/circuits.csv")
display(result)
type(result)
 # infer schema don't use this in production, for the large data this option will slow down the read speed as it consumer extra jobs.

# COMMAND ----------

result.show()

# COMMAND ----------

result.printSchema()

# COMMAND ----------

result.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Reading data by defining the custom schema

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True)
])
# structypes will represent the rows and structfield will represent the columns

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuit_schema) \
    .csv("/mnt/tfstorageisgreat53/raw/circuits.csv")
display(circuits_df)

# COMMAND ----------

# MAGIC %md Specifying particular columns
# MAGIC - Method 1 will only return the records
# MAGIC - 2,3,4 Will allow us to write any column based functions on the record as well like update

# COMMAND ----------

newData=circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country') #method 1
display(newData)

# COMMAND ----------

newData=circuits_df.select(circuits_df.circuitId, 
                           circuits_df.circuitRef, 
                           circuits_df.name, 
                           circuits_df.location, 
                           circuits_df.country) #method 2
display(newData)

# COMMAND ----------

newData=circuits_df.select(circuits_df["circuitId"], 
                           circuits_df["circuitRef"], 
                           circuits_df["name"], 
                           circuits_df["location"], 
                           circuits_df["country"]) #method 3
display(newData)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

circuit_df_selected=circuits_df.select(col("circuitId"),
                                       col("circuitRef"),
                                       col("name"),
                                       col("location"),
                                       col("country"),
                                       col("lat"),
                                       col("lng"),
                                       col("alt")) #this will update the column name
display(circuit_df_selected) #method 4

# COMMAND ----------

# MAGIC %md
# MAGIC column rename as required

# COMMAND ----------

renamedDf=circuit_df_selected.withColumnRenamed('circuitId', 'circuit_id') \
                             .withColumnRenamed('circuitRef', 'circuit_ref') \
                             .withColumnRenamed('lat', 'latitude') \
                             .withColumnRenamed('lng', 'longitude') \
                             .withColumnRenamed('alt', 'altitude')           
display(renamedDf) #this will rename the value

# COMMAND ----------

temp_circuits_final=renamedDf.withColumn("ingestion_date", current_timestamp())\
.withColumn('env', lit('Production')) #this will add extra column in existing table & lit will help to add liternal value
display(temp_circuits_final)

# COMMAND ----------

circuits_final=renamedDf.withColumn("ingestion_date", current_timestamp())
display(circuits_final)

# COMMAND ----------

# MAGIC %md
# MAGIC Writing data to data lake as parquet file

# COMMAND ----------

circuits_final.write.mode('overwrite').parquet('/mnt/tfstorageisgreat53/processed/circuits') #exporting table as parquet file to another container

# COMMAND ----------

df=spark.read.parquet('/mnt/tfstorageisgreat53/processed/circuits/')
display(df) #reading the data from parquet file

# COMMAND ----------

# MAGIC %md
# MAGIC course assignment
# MAGIC - rename the column names
# MAGIC - add new column

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("year", IntegerType(), True),
                                 StructField("round", DoubleType(), True),
                                 StructField("circuitId", IntegerType(),True),
                                 StructField("name", StringType(), True),
                                 StructField("date", DateType(), True),
                                 StructField("time", TimestampType(), True),
                                 StructField("url", StringType(), True)
                                ])
# defining schema

# COMMAND ----------

race_info = spark.read.option('header', True) \
    .schema(race_schema) \
    .csv('/mnt/tfstorageisgreat53/raw/races.csv')
display(race_info)
race_info.printSchema()

# COMMAND ----------

race_data_selected=race_info.select(col('raceId'),
                                    col('year'),
                                    col('round'),
                                    col('circuitId'),
                                    col('name'),
                                    col('date'),
                                    col('time'))
race_data_selected.printSchema()

# COMMAND ----------

race_result=race_data_selected.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(''), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(race_result)

# COMMAND ----------


