import os, json, time, math
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta

from kafka import KafkaConsumer, KafkaProducer

BROKER      = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC    = os.getenv("WATCH_TOPIC", "watchlist.events")
OUT_TOPIC   = os.getenv("ALERT_TOPIC", "alerts.spikes")

WINDOW_MIN  = int(os.getenv("WINDOW_MIN", "15"))
THRESHOLD_X = float(os.getenv("THRESHOLD_X", "3.0"))  # spike if now > X * baseline
NEGATIVE_CUTOFF = float(os.getenv("NEGATIVE_CUTOFF", "-0.3"))  # consider as negative

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def make_consumer():
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BROKER],
        group_id="spike-detector",
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

def main():
    consumer = make_consumer()
    producer = make_producer()

    # For each watch_key, keep a deque of timestamps for NEGATIVE items
    windows = defaultdict(deque)
    # Rolling baseline (simple EMA per key)
    ema = defaultdict(float)
    alpha = 0.1

    span = timedelta(minutes=WINDOW_MIN)

    for msg in consumer:
        doc = msg.value
        key = (doc.get("watch_key") or "").upper()
        sent = float(doc.get("sentiment") or 0.0)
        ts_str = doc.get("published_at") or iso_now()
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(timezone.utc)

        # consider only negative-ish items
        if sent > NEGATIVE_CUTOFF:
            continue

        dq = windows[key]
        dq.append(ts)

        # drop old entries
        cutoff = ts - span
        while dq and dq[0] < cutoff:
            dq.popleft()

        # current count
        cnt_now = len(dq)
        # update baseline
        ema[key] = alpha * cnt_now + (1 - alpha) * ema[key]
        baseline = max(1.0, ema[key])  # avoid div-by-zero

        if cnt_now > THRESHOLD_X * baseline:
            alert = {
                "watch_key": key,
                "type": "negative_spike",
                "window_minutes": WINDOW_MIN,
                "count_now": cnt_now,
                "baseline_avg": round(baseline, 2),
                "ratio": round(cnt_now / baseline, 2),
                "generated_at": iso_now(),
            }
            producer.send(OUT_TOPIC, value=alert)
            print(f"[spike] ALERT {alert}")

    producer.flush()

if __name__ == "__main__":
    main()
