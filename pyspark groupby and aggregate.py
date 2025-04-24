from pyspark.sql import SparkSession
spark= SparkSession.builder.appName('Databricks').getOrCreate()
spark
df_spark= spark.read.csv('/FileStore/tables/test3.csv',header='True',inferSchema='True')
df_spark.show()
df_spark.printSchema()
##Group By
##Grouped to find maximum slary
df_spark.groupBy('name').sum().show()
### Group By departments which gives maximum salary
df_spark.groupBy('Departments').sum().show()
df_spark.groupBy('Departments').count().show()
df_spark.agg({'salary':'sum'}).show()
from pyspark.sql import SparkSession
spark= SparkSession.builder.appName('Databricks').getOrCreate()
training= spark.read.csv("/FileStore/tables/test1.csv",header='True',inferSchema='True')
training= spark.read.csv("/FileStore/tables/test1.csv",header='True',inferSchema='True')
training.show()
training.columns
