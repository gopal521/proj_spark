!pip install pyspark
import pyspark
df_spark=spark.read.format("csv")\
         .option("header","True")\
         .option("inferschema","True")\
         .option("mode","FAILFAST")\
         .load("/FileStore/tables/test1.csv")
df_spark.show()




df_spark.printSchema()
import pandas as pd
df_spark = spark.read.format('csv').options(header='true').load('/FileStore/tables/test1.csv').show

import pyspark
pip install pandas
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
spark=SparkSession.builder.appName('Databricks').getOrCreate()
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
df_spark=spark.read.format("csv")\
            .option("header", "true")\
            .option("mode","FAILFAST")\
            .option("inferschema","true")\
            .load("/FileStore/tables/test1.csv")
df_spark.show()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
SparkContext.getOrCreate()
df_spark.head(3)

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
SparkContext.getOrCreate()


from pyspark.sql import SparkSession

df_spark=spark.read.option("header","True").csv("/FileStore/tables/test1.csv",inferSchema=True)   
df_spark.printSchema()
type(df_spark)
df_spark=spark.read.csv("/FileStore/tables/test1.csv",header=True,inferSchema=True)
df_spark.show()
#get all columns name
df_spark.columns
df_spark.select(['Name','Experience']).show()
df_spark.dtypes
df_spark.describe().show()
###adding columns
df_spark=df_spark.withColumn('Experience after 2 years',df_spark['Experience']+ 2)
df_spark.show()
df_spark=df_spark.drop('Experience after 2 years')
df_spark.show()
##rename columns
df_spark=df_spark.withColumnRenamed('Name','New Name')
df_spark.show()
df_spark=spark.read.format("csv")\
            .option("header", "true")\
            .option("mode","FAILFAST")\
            .option("inferschema","true")\
            .load("/FileStore/tables/test2.csv")
df_spark.show()