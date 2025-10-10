import os
import time
import json
import socket
import feedparser
from datetime import datetime, timezone
from urllib.parse import urlencode
from kafka import KafkaProducer
from dateutil import parser as dtparse

from common_utils import strip_html, stable_id

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC  = os.getenv("RAW_TOPIC", "news.raw")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "45"))

# You can add/remove feeds here
BBC_FEEDS = [
    ("bbc", "World", "http://feeds.bbci.co.uk/news/world/rss.xml"),
    ("bbc", "Technology", "http://feeds.bbci.co.uk/news/technology/rss.xml"),
]
GOOGLE_NEWS_QUERIES = [
    ("google_news", "Artificial Intelligence", {"q": "artificial+intelligence", "hl":"en-US","gl":"US","ceid":"US:en"}),
    ("google_news", "Finance", {"q": "stock+market", "hl":"en-US","gl":"US","ceid":"US:en"}),
]

def google_news_url(params: dict) -> str:
    return f"https://news.google.com/rss/search?{urlencode(params)}"

def iso(dt):
    if not dt:
        return None
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).isoformat()
    try:
        return dtparse.parse(str(dt)).astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()

def make_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
        retries=5,
    )

def iter_feeds():
    # bbc
    for src, section, url in BBC_FEEDS:
        yield src, section, url
    # google news
    for src, section, params in GOOGLE_NEWS_QUERIES:
        yield src, section, google_news_url(params)

def normalize_entry(src, section, entry):
    title = entry.get("title") or ""
    link  = entry.get("link") or entry.get("id") or ""
    summary = entry.get("summary", "") or entry.get("description", "")
    summary = strip_html(summary)

    # published date (RSS has several possible fields)
    published = (
        entry.get("published") or
        entry.get("updated") or
        entry.get("pubDate") or
        None
    )

    doc = {
        "id": stable_id(link),
        "source": src,
        "section": section,
        "title": title.strip(),
        "summary": summary,
        "url": link,
        "published_at": iso(published),
        "lang": "en",   # most of these feeds are en; adjust if you add others
        "ingested_at": iso(datetime.now(timezone.utc)),
        "host": socket.gethostname(),
    }
    return doc

def main():
    producer = make_producer()
    print(f"[rss_producer] broker={BROKER} topic={TOPIC}")
    while True:
        total = 0
        for src, section, url in iter_feeds():
            feed = feedparser.parse(url)
            for entry in feed.entries[:20]:  # cap per poll to be polite
                doc = normalize_entry(src, section, entry)
                producer.send(TOPIC, value=doc)
                total += 1
        producer.flush()
        print(f"[rss_producer] produced {total} messages to {TOPIC}. sleeping {POLL_SECONDS}s…")
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
