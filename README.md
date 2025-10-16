# Mastodon Spark Pipeline

Data streaming project: **Mastodon → Kafka → Spark → PostgreSQL → Visualization**

This project demonstrates a **complete end-to-end data processing pipeline** combining real-time ingestion, batch processing, machine learning–based sentiment analysis, and visual analytics.

---

## Part 1 — Real-Time Ingestion (Mastodon → Kafka)

### Requirements

* Docker Desktop (compatible with Mac/ARM)
* Python 3.10+
* A Mastodon access token (create one at [mastodon.social](https://mastodon.social))
* Kafka topics: `mastodon_stream`, `mastodon_errors`

---

###  Setup & Execution

```bash
# 1️⃣ Start Kafka and Zookeeper
cd docker
docker compose up -d

# 2️⃣ Create required topics
docker compose exec kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic mastodon_stream --replication-factor 1 --partitions 1"
docker compose exec kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic mastodon_errors --replication-factor 1 --partitions 1"

# 3️⃣ Environment configuration
cd ..
cp .env.example .env   # then set your MASTODON_ACCESS_TOKEN

# 4️⃣ Run the Mastodon producer
cd producer
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python mastodon_producer.py
```

✅ Once running, Mastodon data flows from **Producer → Kafka → Spark → PostgreSQL**.

---

###  Real-Time Pipeline Overview

| Component                      | Description                                                |
| ------------------------------ | ---------------------------------------------------------- |
| **Producer**                   | Collects live Mastodon toots and pushes them to Kafka.     |
| **Kafka**                      | Acts as a message broker for reliable event streaming.     |
| **Spark Structured Streaming** | Reads, parses, and aggregates Kafka messages in real time. |
| **PostgreSQL**                 | Stores processed outputs in structured tables.             |

The following tables are generated:

* `masto.toots_raw` → raw toots data
* `masto.toot_metrics_windowed` → language-based metrics (1-minute window)
* `masto.user_avg_length_windowed` → user-level metrics (1-minute window)

---

##  Part 2 — Batch Processing with Spark

## Spark Web UI Access

Once the Spark container is running, you can access the Spark Web UI in your browser to monitor jobs, stages, and executors.

- **Spark Master UI:**  
  [http://localhost:8080](http://localhost:8080)

- **Spark Worker / Jobs UI:**  
  [http://localhost:4040](http://localhost:4040)

> ℹ️ The Spark UI shows real-time metrics, DAGs, and execution details of the streaming and batch jobs.  
> This interface is automatically accessible when the Spark container is up via Docker Compose.


To perform deeper analyses, batch jobs are scheduled to compute aggregate metrics from the raw data.

```bash
cd docker
docker compose exec spark bash -lc '
mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2
export PG_URL="jdbc:postgresql://postgres:5432/airflow"
export PG_USER="airflow"
export PG_PASS="airflow"
export ACTIVITY_MIN_TOOTS=5
/opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --name MastodonBatchAnalysis \
  --packages org.postgresql:postgresql:42.7.4 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /app/batch_job.py
'
```

---

##  Part 3 — Sentiment Analysis with Spark MLlib

### Objective

Build and evaluate a **sentiment analysis model** using **Spark MLlib**, then apply it to Mastodon toots previously collected and stored in PostgreSQL.

The results are stored in a new table for visualization and insights.

---

### Steps Overview

#### 1️⃣ Text Preprocessing

* Clean `content_txt` field
* Tokenize words (`RegexTokenizer`)
* Remove stopwords (`StopWordsRemover`)
* Compute TF-IDF features (`HashingTF` + `IDF`)

#### 2️⃣ Model Training

* Algorithm: `LogisticRegression`
* Dataset: [Sentiment140 (Kaggle)](https://www.kaggle.com/datasets/kazanova/sentiment140)
* Train/test split: **80/20**
* Model saved to:

  ```
  /data/models/sentiment_pipeline_model
  ```

#### 3️⃣ Evaluation

| Metric       | Value  |
| :----------- | :----- |
| **AUC**      | 0.8169 |
| **Accuracy** | 0.7590 |
| **F1-score** | 0.7590 |

#### 4️⃣ Batch Prediction

* Reads data from `masto.toots_raw`
* Applies the trained model
* Extracts `prob_pos` (positive sentiment probability)
* Assigns:

  * `positive` if ≥ 0.55
  * `negative` if ≤ 0.45
  * `neutral` otherwise
* Writes output to:

  ```
  masto.toots_sentiment
  ```

---

### Database Schema

#### Table: `masto.toots_sentiment`

| Column     | Type      | Description                          |
| :--------- | :-------- | :----------------------------------- |
| id         | bigint    | Unique toot ID                       |
| created_at | timestamp | Publication date                     |
| lang       | text      | Language                             |
| text       | text      | Cleaned toot text                    |
| label_bin  | int       | 0 = negative / 1 = positive          |
| prob_pos   | float     | Positive sentiment probability       |
| label_str  | text      | "positive", "neutral", or "negative" |

#### View: `masto.v_sentiment_daily`

```sql
CREATE OR REPLACE VIEW masto.v_sentiment_daily AS
SELECT date_trunc('day', created_at)::date AS day,
       COUNT(*) AS n_toosts,
       SUM((label_str='positive')::int) AS n_pos,
       SUM((label_str='negative')::int) AS n_neg,
       SUM((label_str='neutral')::int)  AS n_neu,
       AVG(prob_pos) AS avg_prob_pos
FROM masto.toots_sentiment
GROUP BY 1
ORDER BY 1;
```

---

##  Part 4 — Running the Model

### 🔹 Train the Sentiment Model

```bash
docker compose exec spark bash -lc '
mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2
export DATA_PATH="/data/training.1600000.processed.noemoticon.csv"
export MODEL_DIR="/data/models/sentiment_pipeline_model"
/opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --name MastodonSentimentTrain \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /app/sentiment_train.py
'
```

Expected output:

```
✅ Model saved at /data/models/sentiment_pipeline_model
AUC: 0.8169
Accuracy: 0.7590
F1: 0.7590
```

### Run Batch Sentiment Prediction

```bash
docker compose exec spark bash -lc '
mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2
export MODEL_DIR="/data/models/sentiment_pipeline_model"
export PG_URL="jdbc:postgresql://postgres:5432/airflow"
export PG_USER="airflow"
export PG_PASS="airflow"
/opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --name MastodonSentimentBatchPredict \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.4 \
  /app/sentiment_batch_predict.py
'
```

Expected result:

```
✅ Batch prediction completed and written to masto.toots_sentiment
```

---

##  Part 5 — Visualization of Results

This final stage performs **data visualization** using **Pandas** and **Matplotlib** to analyze the sentiment trends and popular topics.

### Output Directory

All generated plots are automatically saved under:

```
./reports/
```

###  Generated Charts

| File                                   | Description                                                                      |
| -------------------------------------- | -------------------------------------------------------------------------------- |
| **part5_sentiment_distribution.png**   | Overall distribution of positive, neutral, and negative sentiments.              |
| **part5_sentiment_by_day_stacked.png** | Daily stacked sentiment evolution.                                               |
| **part5_toots_per_day.png**            | Number of toots per day *(empty here since only one day of data was available)*. |
| **part5_top_hashtags.png**             | Top 30 most frequent hashtags.                                                   |

###  Insights

* Most Mastodon toots are **positive**, with fewer negatives and neutrals.
* Top hashtags relate to art, music, and technology (e.g., `#cheerlights`, `#nowplaying`).
* Despite a small dataset, the results prove the **pipeline’s end-to-end functionality**: ingestion → processing → prediction → visualization.

---

##  Part 6 — Conclusion & Future Work

This **Mastodon Spark Pipeline** demonstrates a modern **distributed ETL architecture** capable of processing social media data in real-time and batch modes.

###  Key Achievements

* End-to-end working pipeline (Kafka → Spark → PostgreSQL → Visualization)
* Successful machine learning–based sentiment analysis
* Automated SQL aggregation views and CSV exports
* Clean, reproducible Docker-based setup

### Possible Improvements

* Implement **real-time sentiment tracking** with Spark Streaming
* Deploy on **cloud platforms (AWS/GCP/Azure)**
* Build **interactive dashboards** (Streamlit, Superset, Power BI)
* Upgrade to **deep learning models** (e.g., BERT or LSTM) for better accuracy

---
##  Quick Start Guide

To reproduce the full Mastodon → Kafka → Spark → PostgreSQL → Visualization pipeline:

```bash
# 1. Start the infrastructure
cd docker
docker compose up -d

# 2. Create Kafka topics (once)
docker compose exec kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic mastodon_stream --replication-factor 1 --partitions 1"
docker compose exec kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic mastodon_errors --replication-factor 1 --partitions 1"

# 3. Start the Mastodon producer
cd ../producer
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python mastodon_producer.py

# 4. Run Spark Streaming to process and store toots
cd ../docker
docker compose exec spark bash -lc '/opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.4 \
  /app/streaming_job.py'

# 5. Run batch processing and sentiment analysis (optional)
docker compose exec spark bash -lc '/opt/spark/bin/spark-submit --master spark://spark:7077 /app/batch_job.py'
docker compose exec spark bash -lc '/opt/spark/bin/spark-submit --master spark://spark:7077 /app/sentiment_batch_predict.py'

# 6. Generate visualizations
python app/viz_part5.py


##  Author

**Aminata Giovanna Sylla**
Master’s in Data Engineering — SUPINFO Lyon

