# import os
# import json
# import sqlite3
# from contextlib import closing
# from kafka import KafkaConsumer, KafkaProducer
# from common_utils import strip_html

# BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC  = os.getenv("RAW_TOPIC", "news.raw")
# OUT_TOPIC = os.getenv("CLEAN_TOPIC", "news.cleaned")
# STATE_DIR = os.getenv("STATE_DIR", "state")
# DB_PATH   = os.path.join(STATE_DIR, "seen.db")

# os.makedirs(STATE_DIR, exist_ok=True)

# def init_db():
#     with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS seen (
#                 id TEXT PRIMARY KEY
#             )
#         """)
#         conn.commit()

# def seen_before(doc_id: str) -> bool:
#     with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as cur:
#         cur.execute("SELECT 1 FROM seen WHERE id = ? LIMIT 1", (doc_id,))
#         row = cur.fetchone()
#         return row is not None

# def mark_seen(doc_id: str):
#     with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
#         cur.execute("INSERT OR IGNORE INTO seen(id) VALUES(?)", (doc_id,))
#         conn.commit()

# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id="cleaner-service",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         consumer_timeout_ms=30000,
#         max_poll_records=200,
#     )

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks="all",
#         linger_ms=50,
#         retries=5,
#     )

# def normalize(doc: dict) -> dict:
#     doc = dict(doc)  # shallow copy
#     doc["title"]   = (doc.get("title") or "").strip()
#     doc["summary"] = strip_html(doc.get("summary") or "")
#     doc["url"]     = (doc.get("url") or "").strip()
#     doc["source"]  = (doc.get("source") or "").strip()
#     doc["section"] = (doc.get("section") or "").strip()
#     doc["lang"]    = (doc.get("lang") or "en").strip()
#     return doc

# def main():
#     init_db()
#     consumer = make_consumer()
#     producer = make_producer()
#     print(f"[cleaner] consuming from {IN_TOPIC} → producing to {OUT_TOPIC}")

#     processed = 0
#     deduped = 0

#     for msg in consumer:
#         doc = msg.value
#         doc_id = doc.get("id") or ""
#         if not doc_id:
#             continue  # skip malformed
#         if seen_before(doc_id):
#             deduped += 1
#             continue
#         clean = normalize(doc)
#         producer.send(OUT_TOPIC, value=clean)
#         mark_seen(doc_id)
#         processed += 1

#         if (processed + deduped) % 50 == 0:
#             print(f"[cleaner] processed={processed}, deduped={deduped}")

#     producer.flush()
#     print(f"[cleaner] done. total processed={processed}, deduped={deduped}")

# if __name__ == "__main__":
#     main()













import os
import json
import sqlite3
import re
from contextlib import closing
from kafka import KafkaConsumer, KafkaProducer

# --- CONFIG ---
BLACKLIST_KEYWORDS = [
    "porn", "xxx", "sex", "adult video", "nsfw", "camgirl", "erotic", "nude"
]

def strip_html(text):
    if not text: return ""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def is_safe_content(doc: dict) -> bool:
    """Returns False if content contains explicitly banned keywords."""
    text = (doc.get("title", "") + " " + doc.get("summary", "") + " " + doc.get("url", "")).lower()
    for bad_word in BLACKLIST_KEYWORDS:
        if bad_word in text:
            return False
    return True

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("RAW_TOPIC", "news.raw")
OUT_TOPIC = os.getenv("CLEAN_TOPIC", "news.cleaned")
STATE_DIR = os.getenv("STATE_DIR", "state")
DB_PATH   = os.path.join(STATE_DIR, "seen.db")

os.makedirs(STATE_DIR, exist_ok=True)

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY)")
        conn.commit()

def seen_before(doc_id: str) -> bool:
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as cur:
        cur.execute("SELECT 1 FROM seen WHERE id = ? LIMIT 1", (doc_id,))
        return cur.fetchone() is not None

def mark_seen(doc_id: str):
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute("INSERT OR IGNORE INTO seen(id) VALUES(?)", (doc_id,))
        conn.commit()

def make_consumer():
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BROKER],
        group_id="cleaner-service-v2", # New Group ID
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=10000, 
    )

def make_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
    )

def normalize(doc: dict) -> dict:
    doc = dict(doc)
    doc["title"]   = (doc.get("title") or "").strip()
    doc["summary"] = strip_html(doc.get("summary") or "")
    doc["url"]     = (doc.get("url") or "").strip()
    doc["source"]  = (doc.get("source") or "").strip()
    doc["lang"]    = (doc.get("lang") or "en").strip()
    return doc

def main():
    init_db()
    print(f"[cleaner] {IN_TOPIC} -> {OUT_TOPIC}")
    consumer = make_consumer()
    producer = make_producer()

    processed = 0
    deduped = 0
    blocked = 0

    try:
        for msg in consumer:
            doc = msg.value
            doc_id = doc.get("id") or ""
            if not doc_id: continue 

            # 1. Deduplicate
            if seen_before(doc_id):
                deduped += 1
                continue

            # 2. Safety Filter
            if not is_safe_content(doc):
                blocked += 1
                mark_seen(doc_id) # Mark seen so we don't check it again
                # print(f"[cleaner] Blocked explicit content: {doc.get('title')[:30]}...")
                continue

            # 3. Normalize & Send
            clean = normalize(doc)
            producer.send(OUT_TOPIC, value=clean)
            mark_seen(doc_id)
            processed += 1

            if processed % 10 == 0:
                print(f"[cleaner] Processed {processed} (Deduped {deduped})")

    except Exception as e:
        print(f"Error: {e}")

    producer.flush()
    print(f"[cleaner] Done. Processed {processed}, Deduped {deduped}")

if __name__ == "__main__":
    main()










# import os
# import json
# import sqlite3
# from contextlib import closing
# from kafka import KafkaConsumer, KafkaProducer
# from common_utils import strip_html

# BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC = os.getenv("RAW_TOPIC", "news.raw")
# OUT_TOPIC = os.getenv("CLEAN_TOPIC", "news.cleaned")
# STATE_DIR = os.getenv("STATE_DIR", "state")
# DB_PATH   = os.path.join(STATE_DIR, "seen.db")

# os.makedirs(STATE_DIR, exist_ok=True)

# def init_db():
#     with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
#         cur.execute("CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY)")
#         conn.commit()

# def seen_before(doc_id: str) -> bool:
#     with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as cur:
#         cur.execute("SELECT 1 FROM seen WHERE id=? LIMIT 1", (doc_id,))
#         return cur.fetchone() is not None

# def mark_seen(doc_id: str):
#     with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
#         cur.execute("INSERT OR IGNORE INTO seen(id) VALUES(?)", (doc_id,))
#         conn.commit()

# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id="cleaner-service",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         consumer_timeout_ms=30000,
#         max_poll_records=200,
#     )

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks="all",
#         linger_ms=50,
#         retries=5,
#     )

# def normalize(doc: dict) -> dict:
#     d = dict(doc)
#     d["title"]   = (d.get("title") or "").strip()
#     d["summary"] = strip_html(d.get("summary") or "").strip()
#     d["url"]     = (d.get("url") or "").strip()
#     d["source"]  = (d.get("source") or "").strip()
#     d["section"] = (d.get("section") or "").strip()
#     d["lang"]    = (d.get("lang") or "en").strip()
#     return d

# def main():
#     init_db()
#     consumer = make_consumer()
#     producer = make_producer()
#     print(f"[cleaner] consuming from {IN_TOPIC} → producing to {OUT_TOPIC}")

#     processed = deduped = 0
#     for msg in consumer:
#         doc = msg.value
#         doc_id = (doc.get("id") or "").strip()
#         if not doc_id:
#             continue
#         if seen_before(doc_id):
#             deduped += 1
#             continue
#         clean = normalize(doc)
#         producer.send(OUT_TOPIC, value=clean)
#         mark_seen(doc_id)
#         processed += 1
#         if (processed + deduped) % 50 == 0:
#             print(f"[cleaner] processed={processed}, deduped={deduped}")

#     producer.flush()
#     print(f"[cleaner] done. total processed={processed}, deduped={deduped}")

# if __name__ == "__main__":
#     main()
