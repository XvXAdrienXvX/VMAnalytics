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
                    indexer5 = StringIndexer(inputCol='availabilityImpact', outputCol='AvailabilityImpactNumeric')
             
                    encoder1 = OneHotEncoder(inputCol="AttackVectorNumeric", outputCol="AttackVectorEncoded")
                  
                    stages = [indexer1, indexer2, indexer3, indexer4, indexer5, encoder1]

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

        def show_correlation_matrix_heatmap(self, processed_df, column_display_info, width, height, format = ""):   
            try:
                columns = [info['column_name'] for info in column_display_info]
                display_names = [info['display_name'] for info in column_display_info]
                data = processed_df.select(columns)
                # Convert the DataFrame into an RDD of Vectors
                rdd_table = data.rdd.map(lambda row: row[0:])

                # Calculate the Pearson correlation matrix using the RDD of Vectors
                correlation_matrix = Statistics.corr(rdd_table, method="pearson")

                correlation_df = pd.DataFrame(correlation_matrix, columns=columns, index=columns)

                correlation_df.columns = display_names
                correlation_df.index = display_names
                print("Correlation matrix:")
                print(correlation_df)

                # Create a heatmap using Plotly
                fig = px.imshow(correlation_df, zmin=-1, zmax=1)
                fig.update_layout(width=width, height=height, title="Correlation Heatmap")

                # Show the heatmap
                fig.show(format)

            except Exception as e:
                    print(f"An error occurred rendering heat map: {str(e)}")  
                     

        def show_correlation_scatter_plot(self, processed_df, column_1, column_2, xAxis, yAxis, width, height, title, format = ""):   
            try:

                fig = px.scatter(processed_df.toPandas(), x=column_1, y=column_2,
                         labels={column_1: xAxis, column_2: yAxis})

                fig.update_layout(width=width, height=height, title=title)

                fig.show(format)

            except Exception as e:
                    print(f"An error occurred rendering scatter plot: {str(e)}")     

        def show_correlation_stacked_bar_chart(self, processed_df, column_1, column_2, xAxis, yAxis, width, height, title, format = ""):   
            try:

                 grouped_df = processed_df.groupBy(column_1, column_2).count()
                 grouped_df.show()
                # Convert the PySpark DataFrame to a Pandas DataFrame for plotting
                 grouped_pandas_df = grouped_df.toPandas()
                 
                 severity_order = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
                 custom_colors = {
                        "LOW": "green",
                        "MEDIUM": "darkorange",
                        "HIGH": "red",
                        "CRITICAL": "purple"
                    }
                # Create a stacked bar chart using Plotly Express
                 fig = px.bar(
                    grouped_pandas_df,
                    x=column_1,
                    y="count",
                    color=column_2,
                    category_orders={"baseSeverity": severity_order},
                    labels={column_1: xAxis, "count": yAxis + " Count", column_2: yAxis},
                    title=title,
                    color_discrete_map=custom_colors
                 )


                 fig.update_layout(barmode='stack', width=width, height=height)

                 fig.show(format)
            except Exception as e:
                    print(f"An error occurred rendering scatter plot: {str(e)}")       

        def find_mode_category(self, df, filter_col, column, xAxis, yAxis, width, height, format = ""):   
            try:
                
               mode_df  = df.groupBy(filter_col).agg(mode(column).alias(yAxis))
               mode_df = mode_df.withColumnRenamed(filter_col, xAxis)
               mode_pandas = mode_df.toPandas()
               
               mode_df.show()
               # Create a bar chart using Plotly
               fig = px.bar(mode_pandas, x=xAxis, y=yAxis, title=f"Mode of {yAxis} by {xAxis}", width=width, height=height)
              
               fig.show(format)
               
            except Exception as e:
                print(f"An error occurred while calculating mode: {str(e)}")   

        def find_mean_category(self, df, filter_col, column, xAxis, yAxis, width, height, format = ""):   
            try:
               mode_df  = df.groupBy(filter_col).mean(column)
               mode_df = mode_df.withColumnRenamed(filter_col, xAxis)
               mode_df = mode_df.withColumnRenamed(f"avg({column})", yAxis)
               mode_pandas = mode_df.toPandas()
               
               mode_df.show()
               # Create a bar chart using Plotly
               fig = px.bar(mode_pandas, x=xAxis, y=yAxis, title=f"Mean of {yAxis} by {xAxis}", width=width, height=height)
              
               fig.show(format)
               
            except Exception as e:
                print(f"An error occurred while calculating mode: {str(e)}")   

        def show_bubble_chart(self, df, column_1, column_2, xAxis, yAxis, size_col, color_col, width, height, format = ""):   
            try:
                 
                 fig = px.scatter(
                        df.toPandas(),
                        x= column_1,
                        y= column_2,
                        size= size_col,
                        color= color_col,
                        labels={column_1: xAxis, column_2: yAxis},
                        title="Bubble Chart of CVE Data",
                        width=width,
                        height=height
                    )

                 # Show the chart
                 fig.show(format)
               
            except Exception as e:
                print(f"An error occurred while calculating mode: {str(e)}")   