from pyspark.sql import Sparkspark
from queries import q1, q2, q3, q4, q5
import os, sys, time


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def startUp():
    spark = Sparkspark.builder.master("spark://192.168.0.1:7077").appName("ADVDB_Project").getOrCreate()
    return spark


def create_RDD_DF(spark):
    df1 = spark.read.parquet("./input/yellow_taxi_trip_records")
    df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").csv("./input/taxi+_zone_lookup.csv")

    rdd1 = df1.rdd
    rdd2 = df2.rdd

    df1.show()
    df2.show()

    return (df1, df2, rdd1, rdd2)


if __name__ == "__main__":
    spark = startUp()
    dfs = create_RDD_DF(spark)
    query_time = []

    start = time.time()
    q1(spark, dfs[0])
    end = time.time()
    query_time.append(end-start)

    start = time.time()
    q2(spark)
    end = time.time()
    query_time.append(end-start)

    start = time.time()
    q3(spark)
    end = time.time()
    query_time.append(end-start)

    start = time.time()
    q4(spark)
    end = time.time()
    query_time.append(end-start)

    start = time.time()
    q5(spark)
    end = time.time()
    query_time.append(end-start)

    print("henlo wrold")

