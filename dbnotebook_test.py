# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the input and output formats and paths and the table name.
read_format = 'delta'
write_format = 'delta'
load_path = '/databricks-datasets/learning-spark-v2/people/people-10m.delta'
save_path = '/tmp/delta/people-10m'
table_name = 'default.people10m'

# Load the data from its source.
people = spark.read \
    .format(read_format) \
    .load(load_path)

# # Write the data to its target.
# people.write \
#     .format(write_format) \
#     .save(save_path)

# # Create the table.
# spark.sql("CREATE OR REPLACE TABLE " + table_name +
#           " USING DELTA LOCATION '" + save_path + "'")
