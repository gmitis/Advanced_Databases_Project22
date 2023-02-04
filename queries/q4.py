from pyspark.sql.functions import hour,dayofweek, col, row_number
from pyspark.sql.window import Window

def q4(spark, df):
        w2 = Window.partitionBy("weekday").orderBy(col("avg(passenger_count)").desc())
        df.groupBy(hour("tpep_pickup_datetime").alias("hour"), dayofweek("tpep_pickup_datetime").alias("weekday"))\
          .avg("passenger_count")\
          .withColumn("row",row_number().over(w2))\
          .filter(col("row") < 4)\
          .orderBy("weekday","row")\
          .drop("row").show(21)
