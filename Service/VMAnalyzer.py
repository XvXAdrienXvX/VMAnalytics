import pandas as pd
from pyspark.ml.feature import StringIndexer, OneHotEncoder
import plotly.express as px
from pyspark.mllib.stat import Statistics
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf, mode, min, max
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

        def show_heatmap(self, df, width, height, title = "", format = ""):   
            try:

                fig = px.imshow(df, zmin=-1, zmax=1)
                fig.update_layout(width=width, height=height, title=title)

                fig.show(format)

            except Exception as e:
                    print(f"An error occurred rendering heat map: {str(e)}")  

        def calculate_correlation(self, processed_df, column_display_info):   
            try:
                columns = [info['column_name'] for info in column_display_info]
                display_names = [info['display_name'] for info in column_display_info]
                data = processed_df.select(columns)

                rdd_table = data.rdd.map(lambda row: row[0:])

                correlation_matrix = Statistics.corr(rdd_table, method="pearson")

                correlation_df = pd.DataFrame(correlation_matrix, columns=columns, index=columns)

                correlation_df.columns = display_names
                correlation_df.index = display_names
                print("Correlation matrix:")
                print(correlation_df)

                return correlation_df

            except Exception as e:
                    print(f"An error occurred while calculating correlation: {str(e)}")  
                     

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
           
                 grouped_pandas_df = grouped_df.toPandas()
                 
                 severity_order = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
                 custom_colors = {
                        "LOW": "green",
                        "MEDIUM": "darkorange",
                        "HIGH": "red",
                        "CRITICAL": "purple"
                    }
            
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

        def calculate_mode_category(self, df, filter_col, column, filter_col_name, col_name):   
            try:
                
               mode_df  = df.groupBy(filter_col).agg(mode(column).alias(col_name))
               mode_df = mode_df.withColumnRenamed(filter_col, filter_col_name)  
               mode_df.show()
               
               return mode_df
               
            except Exception as e:
                print(f"An error occurred while calculating mode: {str(e)}")   

        def calculate_mean_category(self, df, filter_col, column, filter_col_name, col_name):   
            try:
               mode_df  = df.groupBy(filter_col).mean(column)
               mode_df = mode_df.withColumnRenamed(filter_col, filter_col_name)
               mode_df = mode_df.withColumnRenamed(f"avg({column})", col_name)
               mode_df.show()

               return mode_df
               
            except Exception as e:
                print(f"An error occurred while calculating mean: {str(e)}")   

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

                 fig.show(format)
               
            except Exception as e:
                print(f"An error occurred while calculating mode: {str(e)}")   

        def calculate_range(self, df, columns):   
            try:
                
                aggregation_exprs = []
        
                for col_name in columns:
                    min_expr = min(df[col_name]).alias(f"min_{col_name}")
                    max_expr = max(df[col_name]).alias(f"max_{col_name}")
                    
                    aggregation_exprs.extend([min_expr, max_expr])
                
                result_df = df.select(*aggregation_exprs)
                
                result_df.show()
               
            except Exception as e:
                print(f"An error occurred while calculating range: {str(e)}")   

        def display_table(data):
            print("Attribute   Range      Mean  Mode")

            for item in data:
                attr_name = item["name"].ljust(11)  # Left-align attribute name
                attr_range = f"[{item['range'][0]}, {item['range'][1]}]".ljust(10)
                attr_mean = str(item["mean"]).ljust(6)
                attr_mode = str(item["mode"]).ljust(5)
                
                print(f"{attr_name}  {attr_range}  {attr_mean}  {attr_mode}")

        def show_bar_chart(self, df, xAxis, yAxis, width, height, title = "", format = ""):   
            try:

               fig = px.bar(df.toPandas(), x=xAxis, y=yAxis, title=f"{title}", width=width, height=height)
              
               fig.show(format)
               
            except Exception as e:
                print(f"An error occurred while rendering bar chart: {str(e)}")   
