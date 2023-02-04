def q2(spark, df):    
      df.filter(month("tpep_pickup_datetime")<7).groupBy(month("tpep_pickup_datetime").alias("month")).max("tolls_amount").sort("month").show()
