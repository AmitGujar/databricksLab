# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingeation_date', current_timestamp())
    return output_df

#  this function will take dataframe as input and will add ingestion date column and return the final dataframe

# COMMAND ----------


