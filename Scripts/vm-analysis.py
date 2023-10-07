import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType , FloatType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
import os
import pandas as pd

output_directory = 'C:\Datasets'
csv_file = os.path.join(output_directory, 'vulnerabilities.csv')

vmSchema = StructType([
StructField('id', StringType(), True),
StructField('descriptions', StringType(), True),
StructField('attackVector', StringType(), True),
StructField('vectorString', StringType(), True),
StructField('attackComplexity', StringType(), True),
StructField('confidentialityImpact', StringType(), True),
StructField('integrityImpact', StringType(), True),
StructField('availabilityImpact', StringType(), True),
StructField('baseScore', FloatType(), True),
StructField('baseSeverity', StringType(), True),
StructField('exploitabilityScore', FloatType(), True),
StructField('impactScore', FloatType(), True)
])

spark = SparkSession.builder.appName('PySpark Data Cleaning').getOrCreate()

dataFrame=spark.read.csv(csv_file, header=True,schema=vmSchema)


try:
    # Drop rows with null values
    dataFrame = dataFrame.na.drop()

    # Remove duplicate rows
    dataFrame = dataFrame.dropDuplicates(subset=['id'])

    df2_pandas = dataFrame.toPandas()

    # Define the path for the new CSV file
    output_csv_file = os.path.join(output_directory, 'vulncleaned.csv')

    # Write the df2 DataFrame to the new CSV file
    df2_pandas.to_csv(output_csv_file, index=False)
    print("Distinct count: "+str(dataFrame.count()))

except Exception as e:
    print(f"An error occurred while exporting the DataFrame: {str(e)}")
