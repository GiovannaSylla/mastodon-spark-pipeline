# spark/streaming_job.py
import os
from pyspark.sql import SparkSession, functions as F, types as T

# --- Config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mastodon_stream")

PG_URL  = os.getenv("PG_URL",  "jdbc:postgresql://postgres:5432/airflow")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASS = os.getenv("PG_PASS", "airflow")

# --- Spark ---
spark = (
    SparkSession.builder
        .appName("MastodonStreamProcessing")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- Schéma d'entrée (JSON produit par le producer) ---
schema = T.StructType([
    T.StructField("id",           T.StringType(),  False),
    T.StructField("created_at",   T.StringType(),  True),   # string ISO8601 -> on le convertit
    T.StructField("username",     T.StringType(),  True),
    T.StructField("user_id",      T.StringType(),  True),
    T.StructField("language",     T.StringType(),  True),
    T.StructField("content_html", T.StringType(),  True),
    T.StructField("content",      T.StringType(),  True),
    T.StructField("hashtags",     T.ArrayType(T.StringType()), True),
    T.StructField("favourites",   T.IntegerType(), True),
    T.StructField("reblogs",      T.IntegerType(), True),
])

# --- Helper écriture JDBC ---
def write_pg(df, table):
    (
        df.write
          .format("jdbc")
          .option("url", PG_URL)
          .option("user", PG_USER)
          .option("password", PG_PASS)
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", table)
          .mode("append")
          .save()
    )

# --- Lecture Kafka ---
raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("subscribe", KAFKA_TOPIC)
         .option("startingOffsets", "latest")  # "earliest" si tu veux rejouer l'historique
         .load()
)

# --- Parsing JSON + colonnes utiles ---
parsed = (
    raw.select(F.col("value").cast("string").alias("value"))
       .select(F.from_json("value", schema).alias("j"))
       .select("j.*")
       .withColumn("lang", F.coalesce(F.col("language"), F.lit("und")))
       .withColumn("content_txt", F.col("content"))
       .withColumn("created_at_ts", F.to_timestamp(F.col("created_at")))  # conversion -> timestamp
)

# ================== Flux 1 : RAW ==================
# IMPORTANT : on écrit created_at_ts en tant que created_at (type timestamp)
toots_raw_df = (
    parsed.select(
        F.col("id"),
        F.col("created_at_ts").alias("created_at"),   # <-- cast timestamp vers la table
        F.col("username"),
        F.col("user_id"),
        F.col("lang"),
        F.col("content_txt"),
        F.col("content_html"),
        F.col("hashtags"),
        F.col("favourites"),
        F.col("reblogs"),
    )
)

toots_raw_query = (
    toots_raw_df.writeStream
        .outputMode("append")
        .option("checkpointLocation", "/tmp/chk/toots_raw")
        .foreachBatch(lambda d, _: write_pg(d, "masto.toots_raw"))
        .start()
)

# ================== Flux 2 : métriques par langue ==================
win = F.window(F.col("created_at_ts"), "1 minute", "1 minute")

metrics = (
    parsed
        .where(F.col("created_at_ts").isNotNull())
        .groupBy(win.alias("w"), F.col("lang"))
        .agg(
            F.count(F.lit(1)).alias("toots_count"),
            F.avg(F.length(F.coalesce(F.col("content_txt"), F.lit("")))).alias("avg_length"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("lang"),
            F.col("toots_count"),
            F.col("avg_length"),
        )
)

metrics_query = (
    metrics.writeStream
        .outputMode("update")  # update OK pour foreachBatch
        .option("checkpointLocation", "/tmp/chk/metrics_window")
        .foreachBatch(lambda d, _: write_pg(d, "masto.toot_metrics_windowed"))
        .start()
)

# ================== Flux 3 : moyenne longueur par user ==================
user_avg = (
    parsed
        .where(F.col("created_at_ts").isNotNull())
        .groupBy(win.alias("w"), F.col("username"))
        .agg(
            F.avg(F.length(F.coalesce(F.col("content_txt"), F.lit("")))).alias("avg_length")
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("username"),
            F.col("avg_length"),
        )
)

user_avg_query = (
    user_avg.writeStream
        .outputMode("update")
        .option("checkpointLocation", "/tmp/chk/user_avg_window")
        .foreachBatch(lambda d, _: write_pg(d, "masto.user_avg_length_windowed"))
        .start()
)

spark.streams.awaitAnyTermination()
