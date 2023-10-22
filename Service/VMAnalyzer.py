import pandas as pd
from pyspark.ml.feature import StringIndexer, OneHotEncoder
import plotly.express as px
from pyspark.mllib.stat import Statistics
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf, mode
from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.sql.types import DoubleType
import plotly.graph_objs as go
import numpy as np

class VMAnalyzer():
       
        def __init__(self, spark_session, csv_file: any, vmSchema:any):
             self.spark_session = spark_session
             self.csv_file = csv_file
             self.vmSchema = vmSchema
             self.dataFrame= self.spark_session.read.csv(self.csv_file, header=True,schema=self.vmSchema)
        
        def pre_processing_pipeline(self):   
            try:
                    indexer1 = StringIndexer(inputCol='attackVector', outputCol='AttackVectorNumeric')
                    indexer2 = StringIndexer(inputCol='confidentialityImpact', outputCol='ConfidentialityImpactNumeric')
                    indexer3 = StringIndexer(inputCol='integrityImpact', outputCol='IntegrityImpactNumeric')
                    indexer4 = StringIndexer(inputCol='baseSeverity', outputCol='BaseSeverityNumeric')
             
                    encoder1 = OneHotEncoder(inputCol="AttackVectorNumeric", outputCol="AttackVectorEncoded")
                    # encoder2 = OneHotEncoder(inputCol="ConfidentialityImpactNumeric", outputCol="ConfidentialityImpactEncoded")
                    # encoder3 = OneHotEncoder(inputCol="IntegrityImpactNumeric", outputCol="IntegrityImpactEncoded")
                    # encoder4 = OneHotEncoder(inputCol="BaseSeverityNumeric", outputCol="BaseSeverityEncoded")
                  
                    stages = [indexer1, indexer2, indexer3, indexer4, encoder1]

                    pipeline = Pipeline(stages=stages)
                    model = pipeline.fit(self.dataFrame)
                    transformed_df = model.transform(self.dataFrame)

                    return transformed_df

            except Exception as e:
                    print(f"An error occurred while exporting the DataFrame: {str(e)}")   

        def get_df(self):   
            return self.spark_session.read.csv(self.csv_file, header=True,schema=self.vmSchema)

        def check_correlation(self, processed_df, column_1, column_2):   
            try:
                columns = [column_1,  column_2]
                vector_assembler = VectorAssembler(inputCols=columns, outputCol='features')
                assembled_df = vector_assembler.transform(processed_df)

                correlation_matrix = Correlation.corr(assembled_df, 'features')

              
                correlation_value = correlation_matrix.collect()[0]['pearson(features)'][0, 1]
                print(correlation_value)

            except Exception as e:
                    print(f"An error occurred while performing correlation analysis: {str(e)}")   

        def plot_correlation(self, processed_df, column_1, column_2, width, height):   
            try:
                columns = [column_1,  column_2]
                vector_assembler = VectorAssembler(inputCols=columns, outputCol='features')
                assembled_df = vector_assembler.transform(processed_df)

                correlation_matrix = Correlation.corr(assembled_df, 'features')
                corr_matrix_pd = correlation_matrix.toPandas()
                
                # Create a heatmap using Plotly
                fig = px.imshow(correlation_matrix, zmin=-1, zmax=1, labels=dict(x="Column 1", y="Column 2", color="Correlation"))
                fig.update_layout(width=width, height=height, title="Correlation Heatmap")

                # Show the heatmap
                fig.show()
            

            except Exception as e:
                    print(f"An error occurred rendering scatter plot: {str(e)}")      

        def find_mode_category(self, df, filter_col, column, xAxis, yAxis, width, height):   
            try:
               mode_df  = df.groupBy(filter_col).agg(mode(column).alias(yAxis))
               mode_df = mode_df.withColumnRenamed(filter_col, xAxis)
               mode_pandas = mode_df.toPandas()
               
               mode_df.show()
               # Create a bar chart using Plotly
               fig = px.bar(mode_pandas, x=xAxis, y=yAxis, title=f"Mode of {yAxis} by {xAxis}", width=width, height=height)
              
               fig.show()
               
            except Exception as e:
                print(f"An error occurred while calculating mode: {str(e)}")   