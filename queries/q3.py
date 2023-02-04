from pyspark.sql.window import Window
from pyspark.sql.functions import dayofmonth, avg, year, month, window
import datetime, time


def map_groups(row):
        year = str(year(row.tpep_pickup_datetime))
        month = str(month(row.tpep_pickup_datetime))
        day= str(dayofmonth(row.tpep_pickup_datetime))

        famt = row.fare_amount
        tdist = row.trip_distance

        if day <= 15:
                day1 = 1
                day2 = 15
        else:
                day1 = 16
                day2 = 31

        date = str(year + "-" + month + "-" + day1 + ", " + year + "-" + month +                                                                                                              "-" + day2)            # key is like yy-mm-dd, yy-mm-dd
        return (date , [(famt, 1), (tdist, 1)] )



def dataframe_calc(df):
        start_date = datetime.datetime(2022, 1, 1, 0, 0, 0)
        days_since_1970 = int(time.mktime(start_date.timetuple())/86400)
        offset_days = days_since_1970 % 15

        df.filter(df["PULocationID"] != df["DOLocationID"])\
          .groupBy(window("tpep_pickup_datetime", "15 days", startTime=f"{offset                                                                                                             _days} days").alias("date_interval"))\
          .agg(avg("trip_distance").alias("avg_trip_distance"), \
               avg("fare_amount").alias("avg_fare_amount"))\
          .orderBy("date_interval").show(12, truncate=False)


def rdd_calc(rdd):

        # reduces rdd by key date to a tuple with two tuples: (sumofamount, sumo                                                                                                             famtvalues), (sumoftripdistance, amountOfTripDistanceCells)
        rdd.filter(lambda x: x.PUlocationID != x.DOlocationID)\
           .map(map_groups)\
           .reduceByKey(lambda x,y: (\
                                        (x[0][0]+y[0][0], x[0][1]+y[0][1]),\
                                        (x[1][0]+y[1][0], x[1][1]+y[1][1])\
                                    )\
                        )\
           .mapValues(lambda x: (x[0][0]/x[0][1], x[1][0]/x[1][1]))\
           .collect()



def q3(spark, df, rdd):
        #rdd1 = rdd.filter(lambda x: x.PULocationID != x.DOLocationID).collect()
        #for x in rdd1: print(x)
        # rddex(rdd)
        dataframe_calc(df)
        # rdd_calc(rdd)
