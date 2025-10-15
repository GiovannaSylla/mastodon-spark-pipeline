# spark/batch_job.py
import os
from pyspark.sql import SparkSession, functions as F, types as T

PG_URL  = os.getenv("PG_URL",  "jdbc:postgresql://postgres:5432/airflow")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASS = os.getenv("PG_PASS", "airflow")

ACTIVITY_MIN_TOOTS = int(os.getenv("ACTIVITY_MIN_TOOTS", "5"))

spark = (SparkSession.builder
         .appName("MastodonBatchAnalysis")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

def write_pg(df, table, mode="overwrite"):
    (df.write
       .format("jdbc")
       .option("url", PG_URL)
       .option("user", PG_USER)
       .option("password", PG_PASS)
       .option("driver", "org.postgresql.Driver")
       .option("dbtable", table)
       .option("batchsize", 5000)
       .mode(mode)
       .save())

# --- 1) Charger l'historique depuis Postgres
raw = (spark.read.format("jdbc")
       .option("url", PG_URL)
       .option("user", PG_USER)
       .option("password", PG_PASS)
       .option("driver", "org.postgresql.Driver")
       .option("dbtable", "masto.toots_raw")
       .load())

# Schéma attendu depuis la partie 2 :
# id STRING, created_at TIMESTAMP (timestamptz côté PG), username STRING, user_id STRING,
# lang STRING, content_txt STRING, content_html STRING, hashtags ARRAY<STRING>, favourites INT, reblogs INT

# Colonnes utilitaires pour le batch
df = (raw
      .withColumn("created_at_ts", F.col("created_at").cast(T.TimestampType()))
      .withColumn("day", F.to_date("created_at_ts"))
      .withColumn("hour", F.date_trunc("hour", F.col("created_at_ts")))
      .withColumn("toot_len", F.length(F.coalesce(F.col("content_txt"), F.lit(""))))
     )

# Astuce perfs : partitionner logiquement + mettre en cache (matérialiser)
df = df.repartition("day").cache()
_ = df.count()  # matérialise le cache une fois

# --- 2.a) Activité utilisateurs (utilisateurs avec >= X toots)
user_activity = (df.groupBy("username")
                   .agg(F.count(F.lit(1)).alias("toots_count"),
                        F.avg("toot_len").alias("avg_len"))
                   .filter(F.col("toots_count") >= F.lit(ACTIVITY_MIN_TOOTS))
                )
write_pg(user_activity, "masto.batch_active_users")

# --- 2.b) Comptes par hashtag et par heure + hashtag le plus fréquent
# Exploser proprement les hashtags (tolérer NULL / listes vides)
hashtags = (df
            .withColumn("hashtag", F.explode_outer("hashtags"))
            .withColumn("hashtag", F.lower(F.col("hashtag")))
            .filter(F.col("hashtag").isNotNull() & (F.col("hashtag") != F.lit("")))
           )

hashtag_by_hour = (hashtags
                   .groupBy("hour", "hashtag")
                   .agg(F.count(F.lit(1)).alias("toots_count"))
                  )
# Écriture : peu de lignes par heure -> coalesce(1) pour limiter les petits fichiers côté JDBC
write_pg(hashtag_by_hour.coalesce(1), "masto.batch_hashtag_by_hour")

# Hashtag le plus fréquent global (sur tout l'historique)
top_hashtag_global = (hashtags
                      .groupBy("hashtag")
                      .agg(F.count(F.lit(1)).alias("toots_count"))
                      .orderBy(F.col("toots_count").desc())
                      .limit(1)
                     )
write_pg(top_hashtag_global, "masto.batch_top_hashtag")

# --- 2.c) Agrégations de base par jour
toots_per_day = (df.groupBy("day")
                 .agg(F.count(F.lit(1)).alias("toots_count")))
write_pg(toots_per_day, "masto.batch_toots_per_day")

avg_len_per_day = (df.groupBy("day")
                   .agg(F.avg("toot_len").alias("avg_len")))
write_pg(avg_len_per_day, "masto.batch_avg_len_per_day")

# Exemple d'autre métrique (facultatif) : proportion par langue par jour
by_lang_per_day = (df.groupBy("day", "lang")
                   .agg(F.count(F.lit(1)).alias("toots_count")))
# On peut optimiser l’écriture par clé logique :
write_pg(by_lang_per_day.repartition("day").coalesce(4), "masto.batch_toots_by_lang_per_day")

print("✅ Batch terminé.")
spark.stop()
