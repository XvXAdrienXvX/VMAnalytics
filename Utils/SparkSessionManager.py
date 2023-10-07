from pyspark.sql import SparkSession

class SparkSessionManager:
    @staticmethod
    def create_session():
        return SparkSession.builder.appName('PySpark Data Cleaning').getOrCreate()