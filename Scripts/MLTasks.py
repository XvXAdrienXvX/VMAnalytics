import os
import sys
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql.types import StructType,StructField, StringType, IntegerType , FloatType
from pyspark.sql.functions import col, when
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

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
    StructField('impactScore', FloatType(), False),
    StructField('year', StringType(), True),
    StructField('priority', StringType(), True)
])
    
def encode_df(df):
    indexer1 = StringIndexer(inputCol='attackVector', outputCol='AttackVectorNumeric')
    indexer2 = StringIndexer(inputCol='confidentialityImpact', outputCol='ConfidentialityImpactNumeric')
    indexer3 = StringIndexer(inputCol='integrityImpact', outputCol='IntegrityImpactNumeric')
    indexer4 = StringIndexer(inputCol='baseSeverity', outputCol='BaseSeverityNumeric')
    indexer5 = StringIndexer(inputCol='availabilityImpact', outputCol='AvailabilityImpactNumeric')
    indexer6 = StringIndexer(inputCol="priority", outputCol="priorityNumeric")
    indexer7 = StringIndexer(inputCol='attackComplexity', outputCol='attackComplexityNumeric')

    encoder1 = OneHotEncoder(inputCol="AttackVectorNumeric", outputCol="AttackVectorEncoded")

    stages = [indexer1, indexer2, indexer3, indexer4, indexer5, indexer6, indexer7, encoder1]

    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(dataFrame)
    transformed_df = model.transform(dataFrame)   
    
    return transformed_df

def pre_processing(df):
    immediate_action_thresholds = ['CRITICAL', 'HIGH']
    allocate_threshold = 'MEDIUM'

    df = df.withColumn(
        'priority',
         when(col('baseSeverity').isin(*immediate_action_thresholds), 'Immediate Action')
        .when(col('baseSeverity') == allocate_threshold, 'Allocate')
        .otherwise('Defer')
    )

    df2_pandas = convert_to_pandas(df)
    df2_pandas.to_csv("training_dataset.csv", index=False)
        
def convert_to_pandas(spark_df):
    return spark_df.toPandas()
            
def perform_decision_tree_task(dataFrame):   
    transformed_df = encode_df(dataFrame)

    # VectorAssembler for feature vector
    feature_columns = ["AttackVectorEncoded", "attackComplexityNumeric", "baseScore"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(transformed_df)

    # Split the data into training and test sets
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    # Create a Decision Tree model
    dt = DecisionTreeClassifier(featuresCol="features", labelCol="priorityNumeric", maxDepth=5, maxBins=32)

    # Train the model
    dt_model = dt.fit(train_data)
    
    print("Decision Tree Structure:")
    print(dt_model.toDebugString)
    
    # Make predictions on the test set
    predictions = dt_model.transform(test_data)

    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol="priorityNumeric", predictionCol="prediction", metricName="f1")
    f1_score = evaluator.evaluate(predictions)

    evaluator_precision = MulticlassClassificationEvaluator(labelCol="priorityNumeric", predictionCol="prediction", metricName="weightedPrecision")
    precision = evaluator_precision.evaluate(predictions)

    evaluator_recall = MulticlassClassificationEvaluator(labelCol="priorityNumeric", predictionCol="prediction", metricName="weightedRecall")
    recall = evaluator_recall.evaluate(predictions)


    print(f"F1 Score: {f1_score}")
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    
def perform_logistic_regression(dataFrame):
    transformed_df = encode_df(dataFrame)
    
    feature_columns = ["AttackVectorEncoded", "attackComplexityNumeric", "baseScore"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(transformed_df)

    # Train-test split
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    # Logistic Regression model for multiclass classification
    lr = LogisticRegression(featuresCol="features", labelCol="priorityNumeric", maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")

    # Pipeline with Logistic Regression
    pipeline_lr = Pipeline(stages=[lr])

    # Train the model
    lr_model = pipeline_lr.fit(train_data)

    # Make predictions on the test set
    predictions = lr_model.transform(test_data)

    # Evaluate the model using multiclass classification metrics
    evaluator = MulticlassClassificationEvaluator(labelCol="priorityNumeric", predictionCol="prediction", metricName="weightedRecall")
    recall = evaluator.evaluate(predictions)

    evaluator_precision = MulticlassClassificationEvaluator(labelCol="priorityNumeric", predictionCol="prediction", metricName="weightedPrecision")
    precision = evaluator_precision.evaluate(predictions)

    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="priorityNumeric", predictionCol="prediction", metricName="f1")
    f1_score = evaluator_f1.evaluate(predictions)

    print(f"Recall: {recall}")
    print(f"Precision: {precision}")
    print(f"F1 Score: {f1_score}")

    
if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("MLTasks").getOrCreate()

    method = sys.argv[1]
    csv_file = sys.argv[2]

    print(f"file: {csv_file}")
    dataFrame = spark_session.read.csv(csv_file, header=True, schema=vmSchema)
    if method == "preprocessing":
        pre_processing(dataFrame)
    elif method == "decisiontree":
       perform_decision_tree_task(dataFrame)
    elif method == "logisticregression":
       perform_logistic_regression(dataFrame)