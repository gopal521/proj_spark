from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Practise').getOrCreate()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
SparkContext.getOrCreate()
df_spark= spark.read.csv('/FileStore/tables/test2.csv',header=True,inferSchema=True)
df_spark.show()
df_spark.na.drop().show()
df_spark.na.fill('missing values').show()
from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['age', 'Experience', 'Salary'], 
    outputCols=["{}_imputed".format(c) for c in ['age', 'Experience', 'Salary']]
    ).setStrategy("median")
# Add imputation cols to df
imputer.fit(df_spark).transform(df_spark).show()
##treshold
df_spark.na.drop(how="any", thresh=3).show()
##subset
df_spark.na.drop(how="any",subset=['age']).show()
df_spark= spark.read.csv('/FileStore/tables/test2.csv',header=True,inferSchema=True)
df_spark.show()
### Filling the Missing Value
df_spark.na.fill('missing values').show()
