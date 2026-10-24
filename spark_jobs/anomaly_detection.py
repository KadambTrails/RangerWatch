from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

JDBC_URL = "jdbc:postgresql://rw-postgres:5432/rangerwatch"

spark = (
    SparkSession.builder
    .appName("Anomaly Detection")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
    .getOrCreate()
)

movement_df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "movement_metrics")
    .option("user", os.getenv("POSTGRES_USER"))
    .option("password", os.getenv("POSTGRES_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .load()
)

ALERT_COLS = ["animal_id", "event_time", "alert_type", "severity", "alert_message"]


def make_alert(df, alert_type, severity, message):
    """Tag a DataFrame with alert info and keep only the common alert columns."""
    return (
        df.withColumn("alert_type", F.lit(alert_type))
          .withColumn("severity", F.lit(severity))
          .withColumn("alert_message", F.lit(message))
          .select(*ALERT_COLS)
    )


# 1. High movement
high_movement = make_alert(
    movement_df.filter(F.col("distance_km") > 50),
    "HIGH_MOVEMENT", "HIGH",
    "Animal travelled more than 50 km between observations",
)

# 2. Stationary between observations
stationary_df = movement_df.filter(F.col("distance_km") < 0.5)

no_movement = make_alert(
    stationary_df,
    "NO_MOVEMENT", "MEDIUM",
    "Animal has remained stationary.",
)

# 3. Prolonged stationary (aggregated per animal)
prolonged_stationary = (
    stationary_df
    .groupBy("animal_id")
    .agg(
        F.sum("time_gap_hours").alias("stationary_hours"),
        F.max("event_time").alias("event_time"),
    )
    .filter(F.col("stationary_hours") > 24)
)
prolonged_stationary = make_alert(
    prolonged_stationary,
    "PROLONGED_STATIONARY", "HIGH",
    "Animal has remained stationary for more than 24 hours",
)

# 4. Telemetry gaps
large_time_gap = make_alert(
    movement_df.filter(F.col("time_gap_hours") > 24),
    "MISSING_TELEMETRY", "MEDIUM",
    "Telemetry gap exceeds 24 hours",
)

alerts_df = (
    high_movement
    .unionByName(no_movement)
    .unionByName(prolonged_stationary)
    .unionByName(large_time_gap)
)

print("Alerts Found:")
alerts_df.show(20, truncate=False)

(
    alerts_df.write
    .format("jdbc")
    .mode("overwrite")
    .option("truncate", "true")  
    .option("url", JDBC_URL)
    .option("dbtable", "alerts")
    .option("user", os.getenv("POSTGRES_USER"))
    .option("password", os.getenv("POSTGRES_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .save()
)

spark.stop()