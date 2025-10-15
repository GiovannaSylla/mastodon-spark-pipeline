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
