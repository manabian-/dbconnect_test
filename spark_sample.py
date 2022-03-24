# Executing PySpark in VS Code using Databricks Connect
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
nCols = 15
nRows = 5
columns = ["col"+str(i) for i in range(nCols)]
 
# Create a pandas DF
pandas_df = pd.DataFrame(np.random.randint(0,100, size=(nRows,nCols)))
 
# Convert to Spark DF
df = spark.createDataFrame(pandas_df).toDF(*columns)

df.show() 