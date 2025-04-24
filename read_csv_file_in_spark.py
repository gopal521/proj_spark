flight_df=spark.read.format("csv")\
            .option("header", "false")\
            .option("inferschema","false")\
            .option("mode","FAILFAST")\
            .load("/FileStore/tables/2010_summary.csv")

flight_df.show(5)
flight_df_header=spark.read.format("csv")\
            .option("header", "true")\
            .option("inferschema","false")\
            .option("mode","FAILFAST")\
            .load("/FileStore/tables/2010_summary.csv")

flight_df_header.show(5)
flight_df_header.printSchema()
