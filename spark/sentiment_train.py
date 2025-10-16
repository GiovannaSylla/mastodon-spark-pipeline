# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

DATA_PATH = os.getenv("DATA_PATH", "/data/training.1600000.processed.noemoticon.csv")
MODEL_DIR = os.getenv("MODEL_DIR", "/data/models/sentiment_pipeline_model")
SAMPLE_FRACTION = float(os.getenv("SAMPLE_FRACTION", "1.0")) 


spark = (SparkSession.builder
         .appName("MastodonSentimentTrain")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1) Charger CSV (format Sentiment140)
schema = T.StructType([
    T.StructField("target", T.IntegerType(), True),
    T.StructField("ids", T.LongType(), True),
    T.StructField("date", T.StringType(), True),
    T.StructField("flag", T.StringType(), True),
    T.StructField("user", T.StringType(), True),
    T.StructField("text", T.StringType(), True),
])

df = (spark.read
      .option("header", "false")
      .option("multiLine", "false")
      .option("quote", '"')
      .option("escape", '"')
      .option("mode", "DROPMALFORMED")
      .csv(DATA_PATH, schema=schema)
      .select("target", "text"))

# garder que 0 et 4, map -> 0/1
df = df.where(F.col("target").isin(0, 4)).withColumn("label", F.when(F.col("target")==4, F.lit(1.0)).otherwise(F.lit(0.0)))
df = df.select("label", "text").na.drop()
if SAMPLE_FRACTION < 1.0:
    df = df.sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=42)

# 2) Nettoyage léger
clean = (df
    .withColumn("text", F.regexp_replace("text", r"http\S+", " "))
    .withColumn("text", F.regexp_replace("text", r"[@#]\S+", " "))
    .withColumn("text", F.lower(F.col("text")))
)

# 3) Pipeline : Tokenizer -> StopWords -> TF -> IDF -> LR
tokenizer = RegexTokenizer(inputCol="text", outputCol="tokens", pattern=r"\W+")
remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
tf = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=262144)
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=30, regParam=0.001)

pipeline = Pipeline(stages=[tokenizer, remover, tf, idf, lr])

# 4) Split train/test + fit
train, test = clean.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)

# 5) Eval
pred = model.transform(test)
bin_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
mc_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
mc_f1  = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

print("AUC:", bin_eval.evaluate(pred))
print("Accuracy:", mc_acc.evaluate(pred))
print("F1:", mc_f1.evaluate(pred))

# 6) Save model
model.write().overwrite().save(MODEL_DIR)
print(f"Modèle enregistré dans {MODEL_DIR}")

spark.stop()
