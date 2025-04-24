from pyspark.sql import SparkSession
spark= SparkSession.builder.appName("Databricks").getOrCreate()
df_spark= spark.read.csv('/FileStore/tables/test2.csv',header='true',inferSchema='true').show()
