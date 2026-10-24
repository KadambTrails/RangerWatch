from functools import reduce

from pyspark.sql import SparkSession, Window, Column
import pyspark.sql.functions as F
import os

JDBC_URL = "jdbc:postgresql://rw-postgres:5432/rangerwatch"

spark = (
    SparkSession.builder
    .appName("Anomaly Detection")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
    .getOrCreate()
)


def read_jdbc(table):
    """Read a table, view or '(subquery) alias' from Postgres."""
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", os.getenv("POSTGRES_USER"))
        .option("password", os.getenv("POSTGRES_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .load()
    )


movement_df = read_jdbc("movement_metrics")

ALERT_COLS = ["animal_id", "event_time", "alert_type", "severity", "alert_message"]


def make_alert(df, alert_type, severity, message):

    message_col = message if isinstance(message, Column) else F.lit(message)
    return (
        df.withColumn("alert_type", F.lit(alert_type))
          .withColumn("severity", F.lit(severity))
          .withColumn("alert_message", message_col)
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

alert_frames = [high_movement, no_movement, prolonged_stationary, large_time_gap]

POSITION_CONTEXT_SQL = """
(
    SELECT
        np.id,
        np.animal_id,
        np.event_time,
        pa.name AS protected_area,
        (pa.osm_id IS NOT NULL) AS in_protected_area
    FROM public.normalized_positions np
    LEFT JOIN LATERAL (
        SELECT p.osm_id, p.name
        FROM public.protected_areas p
        WHERE ST_Intersects(p.geom, np.geom)
        ORDER BY p.osm_id
        LIMIT 1
    ) pa ON true
) AS position_context
"""

# 5. Left the protected area

area_count = read_jdbc(
    "(SELECT COUNT(*) AS n FROM public.protected_areas) t"
).first()["n"]

if area_count == 0:
    print("protected_areas is empty - skipping LEFT_PROTECTED_AREA check")
else:
    context_df = read_jdbc(POSITION_CONTEXT_SQL)

    w = Window.partitionBy("animal_id").orderBy("event_time")

    left_area_df = (
        context_df
        .withColumn("was_inside", F.lag("in_protected_area").over(w))
        .withColumn("previous_area", F.lag("protected_area").over(w))
        .filter(F.col("was_inside") & ~F.col("in_protected_area"))
    )

    left_area = make_alert(
        left_area_df,
        "LEFT_PROTECTED_AREA", "HIGH",
        F.concat(
            F.lit("Animal left the protected area: "),
            F.coalesce(F.col("previous_area"), F.lit("unknown")),
        ),
    )
    alert_frames.append(left_area)

alerts_df = reduce(lambda a, b: a.unionByName(b), alert_frames)

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