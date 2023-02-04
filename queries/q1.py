def q1(spark, df1, df2):
        df = df1.join(df2, df1.DOLocationID == df2.LocationID , "inner").select(df1.tpep_pickup_datetime.alias("datetime"), "Zone", "tip_amount")
        df.filter((df["Zone"] == "Battery Park") & (df["datetime"].like("%-03-%"))).groupBy().max("tip_amount").show()
    
