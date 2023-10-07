import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType , FloatType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
import os
import pandas as pd

class VMAnalyzer():
       
        def __init__(self, spark_session, path: str, file_name: str):
             self.spark_session = spark_session
             self.path = path
             self.file_name = file_name
        
        def perform_exploratory_analysis(self):   
          pass 