import os, json, re
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

BROKER         = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC       = os.getenv("ENRICHED_TOPIC", "news.enriched")
OUT_TOPIC      = os.getenv("FILTERED_TOPIC", "news.filtered")
WATCHLIST_FILE = os.getenv("WATCHLIST_FILE", "data/watchlist.txt")

def load_watchlist_ordered(path: str) -> list[str]:
    items = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s: items.append(s)
        print(f"[watchlist] loaded {len(items)} terms from {path}")
    except FileNotFoundError:
        print(f"[watchlist] ERROR: {path} not found")
    return items

def term_regex(term: str) -> re.Pattern:
    # word-boundary match, allow spaces in multi-word terms
    tokens = re.findall(r"\w+", term, flags=re.U) or [term]
    pattern = r"\b" + r"\s+".join(map(re.escape, tokens)) + r"\b"
    return re.compile(pattern, flags=re.I)

def build_matchers(terms: list[str]) -> list[tuple[str, re.Pattern]]:
    return [(t.upper(), term_regex(t)) for t in terms]

def make_consumer():
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BROKER],
        group_id="watchlist-filter",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b is not None else None,
        consumer_timeout_ms=30000,
    )

def make_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
        acks="all",
    )

def haystack(doc: dict) -> str:
    return " ".join([
        doc.get("title", ""),
        doc.get("summary", ""),
        " ".join(doc.get("entities", {}).get("ORG", [])),
        " ".join(doc.get("entities", {}).get("PERSON", [])),
    ])

def first_match(doc: dict, matchers: list[tuple[str, re.Pattern]]) -> str | None:
    text = haystack(doc)
    for term_upper, rx in matchers:
        if rx.search(text): return term_upper
    return None

def main():
    print(f"[watchlist] {IN_TOPIC} → {OUT_TOPIC}")
    terms = load_watchlist_ordered(WATCHLIST_FILE)
    matchers = build_matchers(terms)

    consumer, producer = make_consumer(), make_producer()
    total = forwarded = 0

    for msg in consumer:
        total += 1
        doc = msg.value

        match = first_match(doc, matchers)
        if match:
            doc["watchname"] = match             # ← what user wants to see
            doc["watch_key"] = match             # compatibility
            producer.send(OUT_TOPIC, key=match, value=doc)
            forwarded += 1
            # if forwarded <= 3:
                # print(f"[watchlist][debug] match={match} title={doc.get('title','')[:80]!r}")

        if total % 50 == 0:
            print(f"[watchlist] processed={total} forwarded={forwarded}")

    producer.flush()
    print(f"[watchlist] done. total={total}, forwarded={forwarded}")

if __name__ == "__main__":
    main()
