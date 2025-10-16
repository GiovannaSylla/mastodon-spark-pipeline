# Mastodon Spark Pipeline

Projet data streaming : **Mastodon → Kafka → Spark → PostgreSQL → Visualisations**.

## Part 1 — Ingestion temps réel (Mastodon → Kafka)

### Prérequis
- Docker Desktop (Mac/ARM OK)
- Token Mastodon (créez une app sur https://mastodon.social)
- Python 3.10+ (pour le producer local)
- Kafka topics: `mastodon_stream` & `mastodon_errors`

### Démarrage
```bash
# 1) Kafka & Zookeeper
cd docker
docker compose up -d

# 2) Topics
docker compose exec kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic mastodon_stream --replication-factor 1 --partitions 1"
docker compose exec kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic mastodon_errors --replication-factor 1 --partitions 1"

# 3) Config env
cd ..
cp .env.example .env   # puis mettre MASTODON_ACCESS_TOKEN

# 4) Producer
cd producer
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python mastodon_producer.py

# Mastodon → Kafka → Spark → Postgres (Partie 2 Validée)

Pipeline temps réel :
- **Producer** Mastodon → **Kafka** (`mastodon_stream`)
- **Spark Structured Streaming** lit Kafka, parse, agrège
- **Postgres** reçoit :
  - `masto.toots_raw` (raw events)
  - `masto.toot_metrics_windowed` (fenêtre 1 min par langue)
  - `masto.user_avg_length_windowed` (fenêtre 1 min par user)

## Prérequis
- Docker + Docker Compose
- Accès à un **token Mastodon** (scopes lecture)

## Arborescence
Batch Processing (Partie 3)
Lancer le job
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


## Partie 4 — Sentiment Analysis avec Spark MLlib

### Objectif

Cette partie consiste à construire et évaluer un **modèle d’analyse de sentiments** avec **Spark MLlib**, puis à l’appliquer sur les données Mastodon précédemment collectées.
Les résultats sont enregistrés dans PostgreSQL pour analyse et visualisation.

---

### Étapes réalisées

#### 1. **Prétraitement du texte**

* Nettoyage du texte (`content_txt`)
* Tokenisation des mots avec `RegexTokenizer`
* Suppression des stopwords (`StopWordsRemover`)
* Vectorisation TF-IDF (`HashingTF` + `IDF`)

#### 2. **Entraînement du modèle**

* Modèle : `LogisticRegression`
* Dataset utilisé : `sentiment140` (Kaggle)
* Split 80% / 20% pour l’entraînement et le test
* Sauvegarde du pipeline entraîné dans :

  ```
  /data/models/sentiment_pipeline_model
  ```

#### 3. **Évaluation**

Le modèle a été évalué à l’aide des métriques suivantes :

| Metric       | Score  |
| :----------- | :----- |
| **AUC**      | 0.8169 |
| **Accuracy** | 0.7590 |
| **F1**       | 0.7590 |

Ces résultats sont visibles dans les logs d’exécution du script `sentiment_train.py`.

#### 4. **Batch Sentiment Analysis**

* Lecture de la table PostgreSQL `masto.toots_raw`
* Application du modèle sur chaque toot
* Extraction de la probabilité positive (`prob_pos`)
* Attribution du label :

  * `positive` si `prob_pos >= 0.55`
  * `negative` si `prob_pos <= 0.45`
  * `neutral` sinon
* Écriture dans la table :

  ```
  masto.toots_sentiment
  ```

---

### Structure des tables

#### **Table : masto.toots_sentiment**

| Colonne    | Type      | Description                       |
| :--------- | :-------- | :-------------------------------- |
| id         | bigint    | Identifiant du toot               |
| created_at | timestamp | Date de publication               |
| lang       | text      | Langue                            |
| text       | text      | Contenu textuel                   |
| label_bin  | int       | 0 = neg / 1 = pos                 |
| prob_pos   | float     | Probabilité positive              |
| label_str  | text      | `positive`, `neutral`, `negative` |

#### **Vue : masto.v_sentiment_daily**

Vue d’agrégation journalière des sentiments :

```sql
CREATE OR REPLACE VIEW masto.v_sentiment_daily AS
SELECT date_trunc('day', created_at)::date AS day,
       COUNT(*) AS n_toosts,
       SUM((label_str='positive')::int) AS n_pos,
       SUM((label_str='negative')::int) AS n_neg,
       SUM((label_str='neutral')::int)  AS n_neu,
       AVG(prob_pos) AS avg_prob_pos
FROM masto.toots_sentiment
GROUP BY 1;
```

---

###  Exécution des scripts

#### 1️ Entraînement du modèle

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

 Résultat attendu :

``` Modèle enregistré dans /data/models/sentiment_pipeline_model
AUC: 0.8169
Accuracy: 0.7590
F1: 0.7590
```

#### Batch de prédiction

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

Résultat attendu :

```
Batch de prédiction terminé et écrit dans masto.toots_sentiment
```

---

### Vérification dans PostgreSQL

#### Nombre total de toots analysés :

```bash
docker compose exec postgres bash -lc \
'psql -U airflow -d airflow -c "SELECT COUNT(*) FROM masto.toots_sentiment;"'
```

#### Exemple de résultats :

```bash
docker compose exec postgres bash -lc \
'psql -U airflow -d airflow -c "
  SELECT id, created_at, lang,
         LEFT(text, 80) AS text,
         label_bin,
         ROUND(prob_pos::numeric, 3) AS prob_pos,
         label_str
  FROM masto.toots_sentiment
  ORDER BY created_at DESC
  LIMIT 10;"'
```

#### Vue d’agrégation journalière :

```bash
docker compose exec postgres bash -lc \
'psql -U airflow -d airflow -c "SELECT * FROM masto.v_sentiment_daily ORDER BY day DESC LIMIT 7;"'
```

---

###  Variables d’environnement

| Variable       | Valeur par défaut                         | Description             |
| :------------- | :---------------------------------------- | :---------------------- |
| `MODEL_DIR`    | `/data/models/sentiment_pipeline_model`   | Chemin du modèle MLlib  |
| `PG_URL`       | `jdbc:postgresql://postgres:5432/airflow` | Connexion PostgreSQL    |
| `PG_USER`      | `airflow`                                 | Utilisateur PostgreSQL  |
| `PG_PASS`      | `airflow`                                 | Mot de passe PostgreSQL |
| `NEUTRAL_LOW`  | `0.45`                                    | Seuil inférieur neutre  |
| `NEUTRAL_HIGH` | `0.55`                                    | Seuil supérieur neutre  |

---

