from pyspark.sql.types import StructType,StructField, StringType, IntegerType , FloatType
import os

class DataReader():
       
        def __init__(self, spark_session):
            self.spark_session = spark_session
        
        def read_data(self, path: str, file_name: str):   
          output_directory = path
          csv_file = os.path.join(output_directory, file_name)

          vmSchema = StructType([
          StructField('id', StringType(), False),
          StructField('attackVector', StringType(), False),
          StructField('attackComplexity', StringType(), False),
          StructField('confidentialityImpact', StringType(), False),
          StructField('integrityImpact', StringType(), False),
          StructField('availabilityImpact', StringType(), False),
          StructField('baseScore', FloatType(), False),
          StructField('baseSeverity', StringType(), False),
          StructField('exploitabilityScore', FloatType(), False),
          StructField('impactScore', FloatType(), False)
          ])

          return csv_file, vmSchema
          