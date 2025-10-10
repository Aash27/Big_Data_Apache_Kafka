import os
import json
import sqlite3
from contextlib import closing
from kafka import KafkaConsumer, KafkaProducer
from common_utils import strip_html

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("RAW_TOPIC", "news.raw")
OUT_TOPIC = os.getenv("CLEAN_TOPIC", "news.cleaned")
STATE_DIR = os.getenv("STATE_DIR", "state")
DB_PATH   = os.path.join(STATE_DIR, "seen.db")

os.makedirs(STATE_DIR, exist_ok=True)

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seen (
                id TEXT PRIMARY KEY
            )
        """)
        conn.commit()

def seen_before(doc_id: str) -> bool:
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as cur:
        cur.execute("SELECT 1 FROM seen WHERE id = ? LIMIT 1", (doc_id,))
        row = cur.fetchone()
        return row is not None

def mark_seen(doc_id: str):
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute("INSERT OR IGNORE INTO seen(id) VALUES(?)", (doc_id,))
        conn.commit()

def make_consumer():
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BROKER],
        group_id="cleaner-service",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=30000,
        max_poll_records=200,
    )

def make_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
        retries=5,
    )

def normalize(doc: dict) -> dict:
    doc = dict(doc)  # shallow copy
    doc["title"]   = (doc.get("title") or "").strip()
    doc["summary"] = strip_html(doc.get("summary") or "")
    doc["url"]     = (doc.get("url") or "").strip()
    doc["source"]  = (doc.get("source") or "").strip()
    doc["section"] = (doc.get("section") or "").strip()
    doc["lang"]    = (doc.get("lang") or "en").strip()
    return doc

def main():
    init_db()
    consumer = make_consumer()
    producer = make_producer()
    print(f"[cleaner] consuming from {IN_TOPIC} → producing to {OUT_TOPIC}")

    processed = 0
    deduped = 0

    for msg in consumer:
        doc = msg.value
        doc_id = doc.get("id") or ""
        if not doc_id:
            continue  # skip malformed
        if seen_before(doc_id):
            deduped += 1
            continue
        clean = normalize(doc)
        producer.send(OUT_TOPIC, value=clean)
        mark_seen(doc_id)
        processed += 1

        if (processed + deduped) % 50 == 0:
            print(f"[cleaner] processed={processed}, deduped={deduped}")

    producer.flush()
    print(f"[cleaner] done. total processed={processed}, deduped={deduped}")

if __name__ == "__main__":
    main()
