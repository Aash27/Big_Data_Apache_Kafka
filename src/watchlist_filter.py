import os, json, re, sqlite3
from contextlib import closing
from typing import Set

from kafka import KafkaConsumer, KafkaProducer

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("ENRICHED_TOPIC", "news.enriched")
OUT_TOPIC = os.getenv("WATCH_TOPIC", "watchlist.events")
WATCHLIST_PATH = os.getenv("WATCHLIST_PATH", "data/watchlist.txt")

STATE_DIR = os.getenv("STATE_DIR", "state")
DB_PATH   = os.path.join(STATE_DIR, "watchlist_first_mentions.db")
os.makedirs(STATE_DIR, exist_ok=True)

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS first_seen (
            key TEXT PRIMARY KEY,
            ts  TEXT
        )
        """)
        conn.commit()

def is_first_mention(key: str) -> bool:
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as cur:
        cur.execute("SELECT ts FROM first_seen WHERE key = ? LIMIT 1", (key,))
        row = cur.fetchone()
        return row is None

def mark_seen(key: str, ts: str):
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute("INSERT OR IGNORE INTO first_seen(key, ts) VALUES(?, ?)", (key, ts))
        conn.commit()

def load_watchlist(path: str) -> Set[str]:
    items = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip()
                if t:
                    items.add(t.upper())
    except FileNotFoundError:
        print(f"[watchlist] WARNING: {path} not found. Using empty watchlist.")
    return items

def tokenize(text: str):
    return re.findall(r"[A-Z0-9][A-Z0-9\-&\.]+", (text or "").upper())

def make_consumer():
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BROKER],
        group_id="watchlist-filter",
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

def find_match(watchlist: Set[str], doc: dict):
    title   = (doc.get("title") or "")
    summary = (doc.get("summary") or "")
    entities = doc.get("entities") or {}
    tickers  = doc.get("tickers") or []

    cands = set(tokenize(title)) | set(tokenize(summary)) | {t.upper() for t in tickers}
    for label in ("ORG", "PERSON", "GPE"):
        for val in entities.get(label, []):
            v = val.strip().upper()
            if v:
                cands.add(v)

    for c in cands:
        if c in watchlist:
            return c
    return None

def main():
    init_db()
    watchlist = load_watchlist(WATCHLIST_PATH)
    print(f"[watchlist] loaded {len(watchlist)} items from {WATCHLIST_PATH}")

    consumer = make_consumer()
    producer = make_producer()

    kept = 0
    for msg in consumer:
        doc = msg.value
        key = find_match(watchlist, doc)
        if not key:
            continue

        first = is_first_mention(key)
        if first:
            mark_seen(key, doc.get("published_at") or "")

        out = {
            "watch_key": key,
            "first_mention": bool(first),
            "id": doc.get("id"),
            "title": doc.get("title"),
            "summary": doc.get("summary"),
            "url": doc.get("url"),
            "source": doc.get("source"),
            "published_at": doc.get("published_at"),
            "category": doc.get("category"),
            "sentiment": doc.get("sentiment"),
            "entities": doc.get("entities"),
            "tickers": doc.get("tickers", []),
        }
        producer.send(OUT_TOPIC, value=out)
        kept += 1
        if kept % 10 == 0:
            print(f"[watchlist] forwarded {kept} → {OUT_TOPIC}")

    producer.flush()
    print(f"[watchlist] done. total forwarded={kept}")

if __name__ == "__main__":
    main()
