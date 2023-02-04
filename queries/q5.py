from pyspark.sql.functions import dayofmonth, dayofyear, month, col, row_number
from pyspark.sql.window import Window

def q5(spark, df):
        w2 = Window.partitionBy("month").orderBy(col("avg(ratio)").desc())
        df.filter(col("fare_amount")>0)\
          .withColumn("ratio", col("tip_amount")/ col("fare_amount"))\
          .groupBy(dayofyear("tpep_pickup_datetime").alias("yearday"), month("tpep_pickup_datetime").alias("month"))\
          .avg("ratio")\
          .withColumn("row",row_number().over(w2))\
          .filter(col("row") < 6)\
          .orderBy("month","row")\
          .drop("row")\
          .show(30)
