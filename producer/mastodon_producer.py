import os, json, re, html, sys
from datetime import datetime, timezone
from mastodon import Mastodon, StreamListener
from confluent_kafka import Producer
from dotenv import load_dotenv

# Charger les variables d'environnement (.env)
load_dotenv()

# ---------------------- CONFIGURATION ----------------------

BASE_URL = os.getenv("MASTODON_BASE_URL", "https://mastodon.social")
ACCESS_TOKEN = os.getenv("MASTODON_ACCESS_TOKEN")

# ✅ Correction ici : utiliser kafka:9092 par défaut dans Docker (pas localhost)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "mastodon_stream")
ERR_TOPIC = "mastodon_errors"

KEYWORDS = [
    k.strip().lower()
    for k in os.getenv("FILTER_KEYWORDS", "data,ai,cloud,spark").split(",")
    if k.strip()
]
LANGS = [
    l.strip()
    for l in os.getenv("FILTER_LANGS", "en,fr,und").split(",")
    if l.strip()
]

if not ACCESS_TOKEN:
    print("❌ Missing MASTODON_ACCESS_TOKEN in .env", file=sys.stderr)
    sys.exit(1)

# ---------------------- OUTILS ----------------------

def strip_html(text: str) -> str:
    """Nettoyer le HTML des toots"""
    if not text:
        return ""
    t = re.sub(r"<br\s*/?>", "\n", text)
    t = re.sub(r"<.*?>", "", t)
    return html.unescape(t).strip()

# ✅ Initialisation du producteur Kafka (avec la bonne variable)
try:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    print(f"✅ Connected to Kafka broker at {KAFKA_BOOTSTRAP}")
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}", file=sys.stderr)
    sys.exit(1)

def send(topic: str, doc: dict):
    """Envoi des messages Kafka"""
    try:
        producer.produce(topic, json.dumps(doc).encode("utf-8"))
        producer.poll(0)  # non bloquant
    except Exception as e:
        print(f"⚠️ Kafka produce error: {e}")
        try:
            producer.produce(ERR_TOPIC, json.dumps({"error": str(e)}).encode("utf-8"))
            producer.poll(0)
        except Exception:
            pass

# ---------------------- STREAMING MASTODON ----------------------

class Listener(StreamListener):
    def on_update(self, status):
        try:
            content_html = status.get("content") or ""
            content = strip_html(content_html)
            tags = [t.get("name", "") for t in status.get("tags", [])]
            lang = status.get("language") or "und"
            username = status.get("account", {}).get("acct")
            user_id = str(status.get("account", {}).get("id"))
            favourites = int(status.get("favourites_count", 0))
            reblogs = int(status.get("reblogs_count", 0))
            created_at = status.get("created_at")

            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            else:
                created_at = datetime.now(timezone.utc)

            doc = {
                "id": str(status.get("id")),
                "created_at": created_at.astimezone(timezone.utc).isoformat(),
                "username": username,
                "user_id": user_id,
                "language": lang,
                "content_html": content_html,
                "content": content,
                "hashtags": tags,
                "favourites": favourites,
                "reblogs": reblogs,
            }

            text_lc = content.lower()
            hashtag_lc = [h.lower() for h in tags]
            keep = (
                (not KEYWORDS)
                or any(k in text_lc for k in KEYWORDS)
                or any(k in hashtag_lc for k in KEYWORDS)
            )

            if ((not LANGS) or (lang in LANGS)) and keep:
                send(TOPIC, doc)
        except Exception as e:
            send(ERR_TOPIC, {"error": str(e)})

# ---------------------- MAIN ----------------------

if __name__ == "__main__":
    print(f"Connecting to Mastodon at {BASE_URL}")
    masto = Mastodon(access_token=ACCESS_TOKEN, api_base_url=BASE_URL)
    try:
        masto.stream_public(Listener(), run_async=False)
    except Exception as e:
        print(f"Mastodon streaming error: {e}", file=sys.stderr)
        sys.exit(1)
