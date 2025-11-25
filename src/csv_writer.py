# import os, json, csv, re
# from kafka import KafkaConsumer
# from dotenv import load_dotenv

# load_dotenv()

# BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC  = os.getenv("FILTERED_TOPIC", "news.filtered")  # IMPORTANT: filtered
# CSV_FILE  = os.getenv("CSV_OUTPUT", "data/tech_news.csv")
# GROUP_ID  = f"csv-writer-{os.getpid()}"
# SUMMARY_WORD_LIMIT = int(os.getenv("SUMMARY_WORD_LIMIT", "100"))

# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id=GROUP_ID,
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         key_deserializer=lambda b: b.decode("utf-8") if b is not None else None,
#         consumer_timeout_ms=30000,
#     )

# def clean_summary(text: str) -> str:
#     text = text or ""
#     text = re.sub(r'<[^>]+>', '', text)
#     text = re.sub(r'&[a-zA-Z]+;', '', text)
#     return re.sub(r'\s+', ' ', text).strip()

# def truncate_words(text: str, max_words: int) -> str:
#     words = (text or "").split()
#     return " ".join(words[:max_words]) if len(words) > max_words else (text or "")

# def ensure_parent_dir(path: str):
#     parent = os.path.dirname(path)
#     if parent: os.makedirs(parent, exist_ok=True)

# def main():
#     print(f"[csv-writer] {IN_TOPIC} → {CSV_FILE}")
#     consumer = make_consumer()
#     ensure_parent_dir(CSV_FILE)

#     with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
#         fieldnames = [
#             "watchname",
#             "id","title","summary","url","source","published_at","category","sentiment"
#         ]
#         w = csv.DictWriter(f, fieldnames=fieldnames); w.writeheader()

#         seen = set(); count = 0
#         for msg in consumer:
#             doc = msg.value

#             # Watchname from payload (set by filter); fallback to Kafka key
#             watchname = (doc.get("watchname") or msg.key or "").strip()
#             if not watchname:
#                 # If this triggers, your writer is not reading news.filtered
#                 continue

#             title = (doc.get("title") or "").strip()
#             tkey = title.lower()
#             if not tkey or tkey in seen: continue
#             seen.add(tkey)

#             summary = truncate_words(clean_summary(doc.get("summary", "")), SUMMARY_WORD_LIMIT)

#             w.writerow({
#                 "watchname": watchname,
#                 "id": doc.get("id",""),
#                 "title": doc.get("title",""),
#                 "summary": summary,
#                 "url": doc.get("url",""),
#                 "source": doc.get("source","Unknown"),
#                 "published_at": doc.get("published_at",""),
#                 "category": doc.get("category","General Technology"),
#                 "sentiment": doc.get("sentiment","Negative"),
#             })
#             count += 1
#             if count % 10 == 0:
#                 print(f"[csv-writer] wrote {count} rows to {CSV_FILE}")
#                 f.flush()

#         print(f"[csv-writer] done. total rows={count}")
#         print(f"[csv-writer] Output: {CSV_FILE}")

# if __name__ == "__main__":
#     main()











import os, json, csv, re
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("FILTERED_TOPIC", "news.filtered")
CSV_FILE  = os.getenv("CSV_OUTPUT", "data/tech_news.csv")
# Use a static group ID so Kafka remembers where we left off, 
# but we also check the file content to be 100% sure.
GROUP_ID  = "csv-writer-persistent" 

def clean_summary(text):
    text = re.sub(r'<[^>]+>', '', text or "")
    return re.sub(r'\s+', ' ', text).strip()

def load_existing_ids(filepath):
    """Reads the CSV and returns a set of all IDs already written."""
    ids = set()
    if not os.path.exists(filepath):
        return ids
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "id" in row and row["id"]:
                    ids.add(row["id"])
    except Exception as e:
        print(f"[csv-writer] Warning reading existing file: {e}")
    return ids

def main():
    consumer = KafkaConsumer(
        IN_TOPIC, 
        bootstrap_servers=[BROKER], 
        group_id=GROUP_ID,
        auto_offset_reset="earliest", 
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=10000
    )
    
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    
    # 1. Load history to prevent duplicates
    existing_ids = load_existing_ids(CSV_FILE)
    print(f"[csv-writer] Loaded {len(existing_ids)} existing articles from CSV. These will be skipped.")

    fieldnames = ["watchname","id","title","summary","url","source","published_at","category","sentiment"]
    write_header = not (os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0)
    
    mode = "a" # Append mode
    
    with open(CSV_FILE, mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
            print(f"[csv-writer] Created new file: {CSV_FILE}")

        new_count = 0
        skipped_count = 0

        for msg in consumer:
            doc = msg.value
            doc_id = doc.get("id")

            # 2. Strict Check: Is this ID already in the file?
            if doc_id in existing_ids:
                skipped_count += 1
                continue
            
            # Add to set so we don't write it twice in this same session
            existing_ids.add(doc_id)

            w.writerow({
                "watchname": doc.get("watchname", "General"),
                "id": doc_id,
                "title": doc.get("title", ""),
                "summary": clean_summary(doc.get("summary", ""))[:300],
                "url": doc.get("url", ""),
                "source": doc.get("source", ""),
                "published_at": doc.get("published_at", ""),
                "category": doc.get("category", "Tech"),
                "sentiment": doc.get("sentiment", "Neutral")
            })
            new_count += 1
            if new_count % 5 == 0: 
                print(f"[csv-writer] Wrote {new_count} new rows...")
                f.flush()

    print(f"[csv-writer] Done. Added {new_count} rows. Skipped {skipped_count} duplicates.")

if __name__ == "__main__":
    main()
