import os, csv, json
from datetime import datetime, timezone
from kafka import KafkaConsumer

TOPIC   = os.getenv("WATCH_TOPIC", "watchlist.events")
BROKER  = os.getenv("KAFKA_BROKER", "localhost:9092")
OUTDIR  = os.getenv("CSV_OUTDIR", "csv_out")  # put this inside a OneDrive/SharePoint-synced folder if you want PBI auto-refresh

FIELDS = [
    "watch_key", "first_mention", "id", "title", "summary",
    "url", "source", "published_at", "category", "sentiment"
]

os.makedirs(OUTDIR, exist_ok=True)

def file_path_for(ts: datetime) -> str:
    day = ts.astimezone(timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(OUTDIR, f"watchlist_events_{day}.csv")

def open_writer(ts: datetime):
    path = file_path_for(ts)
    new_file = not os.path.exists(path)
    f = open(path, "a", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=FIELDS)
    if new_file:
        w.writeheader()
    return path, f, w

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        group_id="csv-rotating-sink",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    current_ts = datetime.now(timezone.utc)
    current_path, f, writer = open_writer(current_ts)
    print(f"[csv-sink] writing to {current_path} (rotates daily). Ctrl+C to stop.")

    try:
        for msg in consumer:
            doc = msg.value
            # choose a stable timestamp for rotation (prefer published_at)
            ts_str = (doc.get("published_at") or "")
            try:
                # allow Z suffix
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except Exception:
                ts = datetime.now(timezone.utc)

            # rotate if day changed
            if file_path_for(ts) != current_path:
                f.flush(); f.close()
                current_path, f, writer = open_writer(ts)
                print(f"[csv-sink] rotated to {current_path}")

            row = {k: doc.get(k) for k in FIELDS}
            writer.writerow(row)
            # flush quickly so Power BI can see updates; adjust if you want fewer disk writes
            f.flush()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            f.flush(); f.close()
        except Exception:
            pass
        print(f"[csv-sink] closed. latest file: {current_path}")

if __name__ == "__main__":
    main()
