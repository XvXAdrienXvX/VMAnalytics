from pyspark.sql.types import StructType,StructField, StringType, IntegerType , FloatType
import os

class DataReader():
       
        def __init__(self, spark_session):
            self.spark_session = spark_session
        
        def read_data(self, path: str, file_name: str, output_dir: str):   
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

          return csv_file, vmSchema
          