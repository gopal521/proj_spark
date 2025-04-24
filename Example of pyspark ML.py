from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Databricks').getOrCreate()
training= spark.read.csv("/FileStore/tables/test1.csv",header='True',inferSchema='True')
training.show()
from pyspark.ml.feature import VectorAssembler
featureassembler=VectorAssembler(inputCols=["age","Experience"],outputCol="Independent Features")
output= featureassembler.transform(training)
output.show()
output.columns
finalized_data=output.select("Independent Features","Salary")
finalized_data.show()
finalized_data.printSchema
from pyspark.ml.regression import LinearRegression
##train test split
train_data,test_data=finalized_data.randomSplit([0.75,0.25])
regressor=LinearRegression(featuresCol='Independent Features', labelCol='Salary')
regressor=regressor.fit(train_data)
regressor.coefficients
regressor.intercept
pred_results=regressor.evaluate(test_data)
pred_results.meanAbsoluteError,pred_results.meanSquaredError