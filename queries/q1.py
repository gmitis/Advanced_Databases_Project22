def q1(spark, df):
    print("hi 1")
    df.createOrUpdateTempView("taxis")

    # id of trip which element ????
    df.select("VendorID").filter(df["DOLocationID"] == "Battery Park").max("Tip_amount").show()
    sql = spark.sql("SELECT VendorID FROM taxis WHERE DOLocationID == 'Battery Park' ")
    sql.show()
    