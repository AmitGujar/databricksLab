# Databricks notebook source
# MAGIC %md
# MAGIC Read the csv file using spark data frame reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# using databricks widgets
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
v_data_source


# COMMAND ----------

raw_folder_path
# checking if import is successful

# COMMAND ----------

fileMounts=dbutils.fs.mounts()
display(fileMounts)

# COMMAND ----------

result=spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{raw_folder_path}/circuits.csv")
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
    .csv("/mnt/tfstorageisgreat13/raw/circuits.csv")


# COMMAND ----------

# MAGIC %md Specifying particular columns
# MAGIC - Method 1 will only return the records
# MAGIC - 2,3,4 Will allow us to write any column based functions on the record as well like update

# COMMAND ----------

newData=circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country') #method 1

# COMMAND ----------

newData=circuits_df.select(circuits_df.circuitId, 
                           circuits_df.circuitRef, 
                           circuits_df.name, 
                           circuits_df.location, 
                           circuits_df.country) #method 2

# COMMAND ----------

newData=circuits_df.select(circuits_df["circuitId"], 
                           circuits_df["circuitRef"], 
                           circuits_df["name"], 
                           circuits_df["location"], 
                           circuits_df["country"]) #method 3

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
 #method 4

# COMMAND ----------

# MAGIC %md
# MAGIC column rename as required

# COMMAND ----------

renamedDf=circuit_df_selected.withColumnRenamed('circuitId', 'circuit_id') \
                             .withColumnRenamed('circuitRef', 'circuit_ref') \
                             .withColumnRenamed('lat', 'latitude') \
                             .withColumnRenamed('lng', 'longitude') \
                             .withColumnRenamed('alt', 'altitude')           
#this will rename the value

# COMMAND ----------

temp_circuits_final=renamedDf.withColumn("ingestion_date", current_timestamp())\
.withColumn('env', lit('Production')) #this will add extra column in existing table & lit will help to add liternal value


# COMMAND ----------

circuits_final=renamedDf.withColumn("ingestion_date", current_timestamp()).withColumn('data_source', lit(v_data_source)) 

# COMMAND ----------

# MAGIC %md
# MAGIC Writing data to data lake as parquet file

# COMMAND ----------

circuits_final.write.mode('overwrite').parquet('/mnt/tfstorageisgreat13/processed/circuits') #exporting table as parquet file to another container

# COMMAND ----------

df=spark.read.parquet('/mnt/tfstorageisgreat13/processed/circuits/')
display(df) #reading the data from parquet file

# COMMAND ----------

dbutils.notebook.exit("Success")
