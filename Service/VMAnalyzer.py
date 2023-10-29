import pandas as pd
from pyspark.ml.feature import StringIndexer, OneHotEncoder
import plotly.express as px
import plotly.graph_objects as go
from pyspark.mllib.stat import Statistics
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf, mode, min, max, regexp_replace
from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.sql.types import DoubleType
import plotly.graph_objs as go
import numpy as np
import plotly.figure_factory as ff
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA

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

        def show_heatmap(self, df, options):   
            try:

                fig = px.imshow(df, zmin=-1, zmax=1)
                
                label_font = dict(size=options['font_size'], color=options['font_color'])
                tick_font = dict(size=options['value_size'], color=options['value_color'])
                fig.update_xaxes(title_font=label_font, tickfont = tick_font)
                fig.update_yaxes(title_font=label_font, tickfont = tick_font)
                fig.update_layout(width=options['width'], height=options['height'], title=options['title'])

                fig.show(options['format'])

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
                     

        def show_correlation_scatter_plot(self, processed_df, column_1, column_2, options):   
            try:

                    color_mapping = {column_1: 'purple'}
                    fig = px.scatter(processed_df.toPandas(), x=column_1, y=column_2,
                                    labels={column_1: options['xAxis'], column_2: options['yAxis']},
                                    color_discrete_map=color_mapping
                                    )

                    label_font = dict(size=options['font_size'], color=options['font_color'])
                    tick_font = dict(size=12, color='black')
                    fig.update_xaxes(title_font=label_font)

                    # Adjust character spacing for the "Base Score" label
                    fig.update_yaxes(title_text=options['yAxis'], title_font=label_font, tickfont= tick_font)
                    fig.update_xaxes(title_text=options['xAxis'], title_font=label_font, tickfont= tick_font)

                    fig.update_layout(width=options['width'], height=options['height'], title=options['title'])

                    fig.show(options['format'])

            except Exception as e:
                    print(f"An error occurred rendering scatter plot: {str(e)}")     

        def show_correlation_stacked_bar_chart(self, processed_df, column_1, column_2, options):   
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
                    labels={column_1: options['xAxisLabel'], "count": options['yAxisLabel'], column_2: options['yAxisLabel']},
                    title=options['title'],
                    color_discrete_map=custom_colors
                 )
                 label_font = dict(size=options['font_size'], color=options['font_color'])
                 tick_font = dict(size=options['value_size'], color=options['value_color'])
                 fig.update_xaxes(title_text=options['xAxisLabel'], title_font=label_font, tickfont = tick_font)
                 fig.update_yaxes(title_text=options['yAxisLabel'], title_font=label_font, tickfont = tick_font)
               

                 fig.update_layout(barmode='stack', width=options['width'], height=options['height'])

                 fig.show(options['format'])
            except Exception as e:
                    print(f"An error occurred rendering scatter plot: {str(e)}")       

        def calculate_mode_category(self, df, filter_col, column, filter_col_name, col_name):   
            try:
               df = df.withColumn(filter_col, regexp_replace(df[filter_col], '_', ' ')) 
               mode_df  = df.groupBy(filter_col).agg(mode(column).alias(col_name))
               mode_df = mode_df.withColumnRenamed(filter_col, filter_col_name)  
               mode_df.show()
               
               return mode_df
               
            except Exception as e:
                print(f"An error occurred while calculating mode: {str(e)}")   

        def calculate_mean_category(self, df, filter_col, column, filter_col_name, col_name, setOrderBy = False):   
            try:
               df = df.withColumn(filter_col, regexp_replace(df[filter_col], '_', ' ')) 
               if setOrderBy:
                    mode_df  = df.orderBy(filter_col).groupBy(filter_col).mean(column)
               else:
                    mode_df  = df.groupBy(filter_col).mean(column)
        
               mode_df = mode_df.withColumnRenamed(filter_col, filter_col_name)
               mode_df = mode_df.withColumnRenamed(f"avg({column})", col_name)
               mode_df.show()

               return mode_df
               
            except Exception as e:
                print(f"An error occurred while calculating mean: {str(e)}")   

        def show_bubble_chart(self, df, column_1, column_2, options):   
            try:
                 df = df.withColumn(options['color_col'], regexp_replace(df[options['color_col']], '_', ' ')) 
                 df = df.withColumnRenamed(options['color_col'], options['key_label'])
                 fig = px.scatter(
                        df.toPandas(),
                        x= column_1,
                        y= column_2,
                        size= options['size_col'],
                        color= options['key_label'],
                        labels={column_1: options['xAxisLabel'], column_2: options['yAxisLabel']},
                        title=options['title'],
                        width=options['width'],
                        height=options['height']
                    )

                 label_font = dict(size=options['font_size'], color=options['font_color'])
                 tick_font = dict(size=options['value_size'], color=options['value_color'])
                 fig.update_xaxes(title_text=options['xAxisLabel'], title_font=label_font, tickfont = tick_font)
                 fig.update_yaxes(title_text=options['yAxisLabel'], title_font=label_font, tickfont = tick_font)

                 fig.show(options['format'])
               
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

        def show_box_plot_chart(self, df, xAxis, yAxis, width, height, title = "", format = ""):   
            try:

               fig = px.box(df.toPandas(), x=xAxis, y=yAxis, title=f"{title}", width=width, height=height)
              
               fig.show(format)
               
            except Exception as e:
                print(f"An error occurred while rendering box plot: {str(e)}") 

        def show_histogram(self, df, column_1, column_2, options):   
            try:

               fig = px.histogram(df.toPandas(), x=column_1, y=column_2, title=f"{options['title']}", width=options['width'], height=options['height'], barmode="group", color=column_1)
               label_font = dict(size=options['font_size'], color=options['font_color'])
               tick_font = dict(size=options['value_size'], color=options['value_color'])
               fig.update_xaxes(title_text=options['xAxisLabel'], title_font=label_font, tickfont = tick_font)
               fig.update_yaxes(title_text=options['yAxisLabel'], title_font=label_font, tickfont = tick_font)
               
               fig.show(options['format'])
               
            except Exception as e:
                print(f"An error occurred while rendering histogram: {str(e)}") 

        def show_line_chart(self, df, column_1, column_2, options):   
            try:

               fig = px.line(df.toPandas(), x=column_1, y=column_2, width=options['width'], height=options['height'], title=options['title'])
               label_font = dict(size=options['font_size'], color=options['font_color'])
               tick_font = dict(size=options['value_size'], color=options['value_color'])
               fig.update_xaxes(title_text=options['xAxisLabel'], title_font=label_font, tickfont = tick_font)
               fig.update_yaxes(title_text=options['yAxisLabel'], title_font=label_font, tickfont = tick_font)
               fig.show(options['format'])
               
            except Exception as e:
                print(f"An error occurred while rendering line chart: {str(e)}") 

        def show_go_bar_chart_mean_mode(self, df_mode,df_mean, column_1, column_2, options):   
            try:
                mean_df_pandas = df_mean.toPandas()
                mode_df_pandas = df_mode.toPandas()
                mean_trace = go.Bar(
                    x=mean_df_pandas[column_1],
                    y=mean_df_pandas[column_2],
                    name="Mean"
                )

                
                mode_trace = go.Bar(
                    x=mode_df_pandas[column_1],
                    y=mode_df_pandas[column_2],
                    name="Mode"
                )

                
                layout = go.Layout(
                    title=options['title'],
                    xaxis_title=column_1,
                    yaxis_title=column_2,
                    barmode="group" 
                )

                # Create the figure and add the traces
                fig = go.Figure(data=[mean_trace, mode_trace], layout=layout)
                fig.update_layout(
                            width=options['width'],  
                            height=options['height']  
                        )
                # Show the plot
                fig.show(options['format'])
               
            except Exception as e:
                print(f"An error occurred while rendering go bar chart: {str(e)}")   

        def clustering_pipeline(self, processed_df, columns):   
            try:
              
                assembler = VectorAssembler(inputCols=columns, outputCol="features")
                data_df = assembler.transform(processed_df)

                # Scaling the features
                scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
                scaler_model = scaler.fit(data_df)
                data_df = scaler_model.transform(data_df)

                #data_df.show(5)

                wssse_values =[]
                silhouette_scores_dict = {col: [] for col in columns}
                evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='scaled_features', \
                                                metricName='silhouette', distanceMeasure='squaredEuclidean')

                for i in range(2,8):    
                    KMeans_mod = KMeans(featuresCol='scaled_features', k=i)  
                    KMeans_fit = KMeans_mod.fit(data_df)  
                    output = KMeans_fit.transform(data_df)   
                    score = evaluator.evaluate(output)   
                    wssse_values.append(score)  
                    for col in columns:
                        silhouette_scores_dict[col].append(score)
                    print("Silhouette Score:",score)

                silhouette_scores_df = pd.DataFrame(silhouette_scores_dict)
                print(silhouette_scores_df)
                # # Define the K-means clustering model
                # kmeans = KMeans(k=4, featuresCol="scaled_features", predictionCol="cluster")
                # kmeans_model = kmeans.fit(data_df)

                # # Assigning the data points to clusters
                # clustered_data = kmeans_model.transform(data_df)

                # output = KMeans_fit.transform(data_df)
                # wssse = evaluator.evaluate(output)
                # print(f"Within Set Sum of Squared Errors (WSSSE) = {wssse}")

            except Exception as e:
                    print(f"An error occurred while performing clustering: {str(e)}")  

        def pca_pipeline(self, processed_df, columns):   
            try:
                assembler = VectorAssembler(inputCols=columns, outputCol="features")
                assembled_df = assembler.transform(processed_df.select(columns))

                scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
                scaler_model = scaler.fit(assembled_df)
                scaled_df = scaler_model.transform(assembled_df)

                pca = PCA(k=len(columns), inputCol="scaledFeatures", outputCol="pcaFeatures")
                pca_model = pca.fit(scaled_df)
                pca_result = pca_model.transform(scaled_df)

                for i, col in enumerate(columns):
                    pca_result.select(col, "pcaFeatures").show()

            except Exception as e:
                    print(f"An error occurred while performing pca analysis: {str(e)}")  