# sentiment_batch_predict.py
# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array  # évite l'UDF sur probability

MODEL_DIR = os.getenv("MODEL_DIR", "/data/models/sentiment_pipeline_model")
PG_URL  = os.getenv("PG_URL",  "jdbc:postgresql://postgres:5432/airflow")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASS = os.getenv("PG_PASS", "airflow")

# seuils pour "neutre"
LOW  = float(os.getenv("NEUTRAL_LOW",  "0.45"))
HIGH = float(os.getenv("NEUTRAL_HIGH", "0.55"))

spark = (SparkSession.builder
         .appName("MastodonSentimentBatchPredict")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1) Charger le modèle (pipeline entraîné)
model = PipelineModel.load(MODEL_DIR)

# 2) Charger l'historique Mastodon
#    On prend username aussi pour enrichir l'analyse (si la colonne n'existe pas chez toi, enlève-la du select)
toots = (spark.read
         .format("jdbc")
         .option("url", PG_URL)
         .option("user", PG_USER)
         .option("password", PG_PASS)
         .option("driver", "org.postgresql.Driver")
         .option("dbtable", "masto.toots_raw")
         .load()
         .select("id", "created_at", "username", "lang", F.col("content_txt").alias("text"))
         .na.drop(subset=["text"])
        )

# 3) Appliquer le modèle
scored = model.transform(toots)
#    probability est un vecteur Dense/Sparse -> on le convertit en array et on prend l'index 1 (classe positive)
scored2 = (scored
           .withColumn("prob_vec", vector_to_array("probability"))
           .withColumn("prob_pos", F.col("prob_vec")[1])
           .drop("prob_vec"))

# 4) Déterminer le label texte en fonction des seuils
label_str = (F.when(F.col("prob_pos") >= HIGH, F.lit("positive"))
               .when(F.col("prob_pos") <= LOW,  F.lit("negative"))
               .otherwise(F.lit("neutral")))

# 5) Choisir la colonne de prédiction binaire exposée par le pipeline
#    (selon les versions, ça peut être 'prediction' ou déjà 'label')
pred_col = "prediction" if "prediction" in scored2.columns else ("label" if "label" in scored2.columns else None)
if pred_col is None:
    raise RuntimeError("Aucune colonne de prédiction trouvée (ni 'prediction' ni 'label').")

result = (scored2
          .select(
              "id",
              "created_at",
              "username",
              "lang",
              "text",
              # on force un entier 0/1
              F.col(pred_col).cast(T.IntegerType()).alias("label_bin"),
              F.col("prob_pos").cast(T.DoubleType()).alias("prob_pos"),
              label_str.alias("label_str")
          ))

# 6) Ecrire dans une NOUVELLE table pour éviter les conflits de schéma
(result.write
   .format("jdbc")
   .option("url", PG_URL)
   .option("user", PG_USER)
   .option("password", PG_PASS)
   .option("driver", "org.postgresql.Driver")
   .option("dbtable", "masto.toots_sentiment_v2")
   .mode("append")        # 'append' crée la table si elle n'existe pas, sinon ajoute
   .save())

print("✅ Batch de prédiction terminé et écrit dans masto.toots_sentiment_v2")
spark.stop()
