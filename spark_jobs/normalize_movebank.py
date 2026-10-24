from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os 

spark = (
    SparkSession.builder
    .appName("Normalize Movebank")
    .config("spark.jars.packages","org.postgresql:postgresql:42.7.4")
    .getOrCreate()
)

print("Reading Raw events... ")

raw_df = (
    spark.read
    .format("jdbc")
    .option("url",
            "jdbc:postgresql://rw-postgres:5432/rangerwatch")
    .option("dbtable", "raw_events")
    .option("user",os.getenv("POSTGRES_USER"))
    .option("password",os.getenv("POSTGRES_PASSWORD"))
    .option("driver","org.postgresql.Driver")
    .load()      
)

print("Raw schema: ")
raw_df.printSchema()


raw_df = raw_df.withColumn(
    "animal_id",
    split(
        col("source_record_id"),
        "_"
    ).getItem(0)
)

payload_schema = StructType(
    [
        StructField("timestamp",LongType(),True),
        StructField("location_long",DoubleType(),True),
        StructField("location_lat",DoubleType(),True)
    ]
)

parsed_df = raw_df.withColumn(
    "parsed_payload",
    from_json(
        col("payload"), payload_schema
    )
)

normalized_df = parsed_df.select(
    col("animal_id"),
    from_unixtime(col("parsed_payload.timestamp")/1000).cast("timestamp").alias("event_time"),
    col("parsed_payload.location_lat").alias("latitude"),
    col("parsed_payload.location_long").alias("longitude"),
    col("source")
)

normalized_df.write.format("jdbc").mode("append")\
    .option("url", "jdbc:postgresql://rw-postgres:5432/rangerwatch")\
    .option("dbtable", "normalized_positions")\
    .option("user", os.getenv("POSTGRES_USER"))\
    .option("password", os.getenv("POSTGRES_PASSWORD"))\
    .option("driver", "org.postgresql.Driver").save()