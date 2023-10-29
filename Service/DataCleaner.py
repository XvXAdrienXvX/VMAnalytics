import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType , FloatType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains, substring
import os
import pandas as pd

class DataCleaner():
       
        def __init__(self, spark_session):
            self.spark_session = spark_session
        
        def clean_data_from_source(self, path: str, file_name: str, output_dir: str):   
          output_directory = path
          csv_file = os.path.join(output_directory, file_name)

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

          dataFrame= self.spark_session.read.csv(csv_file, header=True,schema=vmSchema)


          try:
              # Drop rows with null values
              dataFrame = dataFrame.na.drop()

              # Remove duplicate rows
              dataFrame = dataFrame.dropDuplicates(subset=['id'])
              
              dataFrame = dataFrame.withColumn('year', substring(col('id'), 5, 4))
            #   network_attacks_df = dataFrame.filter(dataFrame['attackVector'] == 'NETWORK')
            #   local_attacks_df = dataFrame.filter(dataFrame['attackVector'] == 'LOCAL')
            #   physical_attacks_df = dataFrame.filter(dataFrame['attackVector'] == 'PHYSICAL')
            #   adjacent_attacks_df = dataFrame.filter(dataFrame['attackVector'] == 'ADJACENT_NETWORK')
         
            #   network_attacks_sampled = network_attacks_df.sample(False, 25 / network_attacks_df.count())
            #   local_attacks_sampled = local_attacks_df.sample(False, 25 / local_attacks_df.count())
            #   physical_attacks_sampled = physical_attacks_df.sample(False, 25 / physical_attacks_df.count())
            #   adjacent_attacks_sampled = adjacent_attacks_df.sample(False, 25 / adjacent_attacks_df.count())
              #dataFrame = dataFrame.limit(100)
              #combined_df = network_attacks_sampled.union(local_attacks_sampled).union(adjacent_attacks_sampled).union(physical_attacks_sampled)
              dataFrame = dataFrame.drop('descriptions')
              dataFrame = dataFrame.drop('vectorString')
              df2_pandas = dataFrame.toPandas()

              # Define the path for the new CSV file
              output_csv_file = os.path.join(output_directory, output_dir)

              # Write the df2 DataFrame to the new CSV file
              df2_pandas.to_csv(output_csv_file, index=False)
              #print("Distinct count: "+str(local_attacks_df.count()))

          except Exception as e:
              print(f"An error occurred while exporting the DataFrame: {str(e)}")   