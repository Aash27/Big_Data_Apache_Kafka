import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

def json_serializer(value: dict) -> bytes:
    return json.dumps(value).encode("utf-8")

def main():
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=json_serializer,
        linger_ms=50,  # small batching
        retries=3,
        acks="all",
    )

    sample_items = [
        {
            "id": "demo-1",
            "source": "demo",
            "title": "AI regulation talks intensify",
            "summary": "Ministers discuss cross-border AI guidelines.",
            "url": "https://example.com/ai-regulation",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "section": "Technology",
            "lang": "en",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "id": "demo-2",
            "source": "demo",
            "title": "Markets rise on tech rally",
            "summary": "Tech stocks lead gains amid strong earnings.",
            "url": "https://example.com/markets-rise",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "section": "Business",
            "lang": "en",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        },
    ]

    topic = "news.raw"
    for item in sample_items:
        producer.send(topic, value=item)
        print(f"→ produced to {topic}: {item['id']} - {item['title']}")
        time.sleep(0.2)

    producer.flush()
    print("All messages flushed.")

if __name__ == "__main__":
    main()
