from pyspark.sql import *
from pyspark.sql.functions import *
import os


spark = SparkSession.builder.appName("Movement Metrics")\
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")\
        .getOrCreate()

df = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://rw-postgres:5432/rangerwatch")\
    .option("dbtable","normalized_positions")\
    .option("user", os.getenv("POSTGRES_USER"))\
    .option("password",os.getenv("POSTGRES_PASSWORD"))\
    .option("driver", "org.postgresql.Driver")\
    .load()

window_spec = Window.partitionBy("animal_id")\
              .orderBy("event_time")

df = df.withColumn("previous_latitude", lag("latitude").over(window_spec))\
    .withColumn("previous_longitude", lag("longitude").over(window_spec))\
    .withColumn("previous_event_time", lag("event_time").over(window_spec))

df = df.withColumn("distance_km", sqrt(
    ( col("latitude") - col("previous_latitude")) * (col("latitude") - col("previous_latitude") )
        +   
    ( col("longitude") - col("previous_longitude")) * (col("longitude") - col("previous_longitude") )
    ) * 111)

df = df.withColumn("time_gap_hours", 
        ( unix_timestamp("event_time") - unix_timestamp("previous_event_time")) / 3600)

movement_df = df.select("animal_id", "event_time", "latitude", "longitude", "previous_latitude", 
                        "previous_longitude", "previous_event_time", "distance_km", 
                        "time_gap_hours")

movement_df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://rw-postgres:5432/rangerwatch")\
    .option("dbtable","movement_metrics")\
    .option("user", os.getenv("POSTGRES_USER"))\
    .option("password", os.getenv("POSTGRES_PASSWORD"))\
    .option("driver", "org.postgresql.Driver")\
    .mode("append") \
    .save()