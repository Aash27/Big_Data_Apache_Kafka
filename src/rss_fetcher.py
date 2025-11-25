# import os
# import json
# import hashlib
# import feedparser
# from urllib.parse import urlparse
# from datetime import datetime, timezone
# from kafka import KafkaProducer
# from dotenv import load_dotenv

# load_dotenv()

# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# OUT_TOPIC    = os.getenv("CLEAN_TOPIC", "news.cleaned")

# RSS_FEEDS = [
#     "https://techcrunch.com/feed/",
#     "https://www.theverge.com/rss/index.xml",
#     "https://www.wired.com/feed/rss",
#     "https://arstechnica.com/feed/",
#     "https://www.engadget.com/rss.xml",
#     "https://thenextweb.com/feed/",
#     "https://www.bleepingcomputer.com/feed/",
#     "https://venturebeat.com/feed/",
#     "https://techradar.com/rss",
#     "https://www.zdnet.com/news/rss.xml",
#     "https://www.computerworld.com/index.rss",
#     "https://news.ycombinator.com/rss",
#     "https://dev.to/feed",
#     "https://feeds.feedburner.com/TheRegister/headlines",
#     "https://www.techmeme.com/feed.xml",
#     "https://www.artificialintelligence-news.com/feed/",
#     "https://machinelearningmastery.com/feed/",
# ]

# def iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[KAFKA_BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
#         acks="all",
#         retries=5,
#     )

# def stable_id(link: str, guid: str | None, published: str | None) -> str:
#     if guid:
#         return guid.strip()
#     h = hashlib.sha1()
#     h.update((link or "").encode("utf-8"))
#     h.update((published or "").encode("utf-8"))
#     return h.hexdigest()

# def domain_from_url(url: str) -> str:
#     try:
#         host = (urlparse(url).hostname or "").lower()
#         return host or ""
#     except Exception:
#         return ""

# def fetch_from_rss(feed_url: str):
#     try:
#         print(f"[rss] Fetching from {feed_url}...")
#         feed = feedparser.parse(feed_url)
#         out = []
#         for entry in feed.entries:
#             link = (entry.get("link") or "").strip()
#             title = (entry.get("title") or "").strip()
#             if not link or not title:
#                 continue
#             published = entry.get("published") or entry.get("updated") or ""
#             guid = entry.get("id") or entry.get("guid") or None
#             src_title = feed.feed.get("title", "Unknown")

#             _id = stable_id(link, guid, published)
#             fallback_watch_key = domain_from_url(link)

#             out.append({
#                 "id": _id,
#                 "title": title,
#                 "url": link,
#                 "summary": (entry.get("summary") or entry.get("description") or "").strip(),
#                 "source": src_title,
#                 "published_at": published or iso_now(),
#                 "author": (entry.get("author") or "Unknown"),
#                 "fetched_at": iso_now(),
#                 "watch_key": fallback_watch_key,  # will be replaced by watchlist_filter later
#             })
#         print(f"[rss] Got {len(out)} from {feed.feed.get('title', feed_url)}")
#         return out
#     except Exception as e:
#         print(f"[rss] Error fetching {feed_url}: {e}")
#         return []

# def main():
#     print(f"[rss-fetcher] Start")
#     producer = make_producer()

#     all_articles = []
#     for feed_url in RSS_FEEDS:
#         all_articles.extend(fetch_from_rss(feed_url))

#     print(f"[rss-fetcher] Total fetched: {len(all_articles)}")

#     seen = set()
#     unique_articles = []
#     for a in all_articles:
#         key = a["title"].lower().strip()
#         if key and key not in seen:
#             seen.add(key)
#             unique_articles.append(a)

#     print(f"[rss-fetcher] Unique: {len(unique_articles)}")

#     for i, a in enumerate(unique_articles, 1):
#         # temporary key = domain; final key will be the matched watchlist term later
#         producer.send(OUT_TOPIC, key=a["watch_key"], value=a)
#         if i <= 3:
#             print(f"[rss-fetcher][debug] key={a['watch_key']} id={a['id']} url={a['url']} title={a['title'][:60]!r}")

#     producer.flush()
#     print(f"[rss-fetcher] Done. Sent {len(unique_articles)} to {OUT_TOPIC}")

# if __name__ == "__main__":
#     main()













# import os
# import json
# import hashlib
# import feedparser
# from urllib.parse import urlparse
# from datetime import datetime, timezone, timedelta
# from kafka import KafkaProducer
# from dateutil import parser as date_parser
# from dotenv import load_dotenv

# load_dotenv()

# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# OUT_TOPIC    = os.getenv("RAW_TOPIC", "news.raw")

# # CONFIG: Only fetch news published in the last 24 hours
# MAX_AGE_HOURS = 24

# RSS_FEEDS = [
#     "https://techcrunch.com/feed/",
#     "https://www.theverge.com/rss/index.xml",
#     "https://www.wired.com/feed/rss",
#     "https://arstechnica.com/feed/",
#     "https://www.engadget.com/rss.xml",
#     "https://thenextweb.com/feed/",
#     "https://www.bleepingcomputer.com/feed/",
#     "https://venturebeat.com/feed/",
#     "https://techradar.com/rss",
#     "https://www.zdnet.com/news/rss.xml",
#     "https://www.computerworld.com/index.rss",
#     "https://news.ycombinator.com/rss",
#     "https://dev.to/feed",
#     "https://feeds.feedburner.com/TheRegister/headlines",
#     "https://www.techmeme.com/feed.xml",
#     "https://www.artificialintelligence-news.com/feed/",
#     "https://machinelearningmastery.com/feed/",
# ]

# def iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[KAFKA_BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
#         acks="all",
#         retries=5,
#     )

# def stable_id(link: str, guid: str | None) -> str:
#     if guid: return guid.strip()
#     h = hashlib.sha1()
#     h.update((link or "").encode("utf-8"))
#     return h.hexdigest()

# def domain_from_url(url: str) -> str:
#     try:
#         host = (urlparse(url).hostname or "").lower()
#         return host or ""
#     except Exception:
#         return ""

# def is_recent(published_str: str) -> bool:
#     """Returns True if article is within MAX_AGE_HOURS, False otherwise."""
#     if not published_str:
#         return True # If no date, assume new
#     try:
#         pub_date = date_parser.parse(published_str)
#         # Ensure timezone awareness
#         if pub_date.tzinfo is None:
#             pub_date = pub_date.replace(tzinfo=timezone.utc)
        
#         now = datetime.now(timezone.utc)
#         age = now - pub_date
        
#         # Check if age is positive (in past) and less than limit
#         # Also allow small future dates (clock skew)
#         if age < timedelta(hours=MAX_AGE_HOURS) and age > timedelta(hours=-1):
#             return True
#         return False
#     except Exception:
#         # If parsing fails, allow it to be safe
#         return True

# def fetch_from_rss(feed_url: str):
#     try:
#         print(f"[rss] Fetching {feed_url}...")
#         feed = feedparser.parse(feed_url)
#         out = []
#         skipped_old = 0
        
#         for entry in feed.entries:
#             link = (entry.get("link") or "").strip()
#             title = (entry.get("title") or "").strip()
#             if not link or not title:
#                 continue
                
#             published = entry.get("published") or entry.get("updated") or ""
            
#             # --- TIME FILTER ---
#             if not is_recent(published):
#                 skipped_old += 1
#                 continue
#             # -------------------

#             guid = entry.get("id") or entry.get("guid") or None
#             src_title = feed.feed.get("title", "Unknown")
#             _id = stable_id(link, guid)
#             fallback_watch_key = domain_from_url(link)

#             out.append({
#                 "id": _id,
#                 "title": title,
#                 "url": link,
#                 "summary": (entry.get("summary") or entry.get("description") or "").strip(),
#                 "source": src_title,
#                 "published_at": published or iso_now(),
#                 "author": (entry.get("author") or "Unknown"),
#                 "fetched_at": iso_now(),
#                 "watch_key": fallback_watch_key, 
#             })
            
#         print(f"[rss] {feed.feed.get('title', 'Unknown')}: Got {len(out)} recent items (Skipped {skipped_old} old items)")
#         return out
#     except Exception as e:
#         print(f"[rss] Error fetching {feed_url}: {e}")
#         return []

# def main():
#     print(f"[rss-fetcher] Starting batch fetch (Window: {MAX_AGE_HOURS} hours)")
#     producer = make_producer()

#     all_articles = []
#     for feed_url in RSS_FEEDS:
#         all_articles.extend(fetch_from_rss(feed_url))

#     print(f"[rss-fetcher] Total recent fetched: {len(all_articles)}")

#     seen = set()
#     unique_articles = []
#     for a in all_articles:
#         key = a["title"].lower().strip()
#         if key and key not in seen:
#             seen.add(key)
#             unique_articles.append(a)

#     print(f"[rss-fetcher] Unique to send: {len(unique_articles)}")

#     for a in unique_articles:
#         producer.send(OUT_TOPIC, key=a["watch_key"], value=a)

#     producer.flush()
#     print(f"[rss-fetcher] Done. Sent {len(unique_articles)} articles to {OUT_TOPIC}")

# if __name__ == "__main__":
#     main()











import os
import json
import hashlib
import time
import sys
import feedparser
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from dateutil import parser as date_parser
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
OUT_TOPIC    = os.getenv("RAW_TOPIC", "news.raw")

# CONFIG
MAX_AGE_HOURS = 24
# We only wait 2 seconds between websites to prevent getting IP Banned.
# This creates a continuous "Matrix-style" stream of data.
INTER_REQUEST_DELAY = 2.0 

RSS_FEEDS = [
    "https://techcrunch.com/feed/",
    "https://www.theverge.com/rss/index.xml",
    "https://www.wired.com/feed/rss",
    "https://arstechnica.com/feed/",
    "https://www.engadget.com/rss.xml",
    "https://thenextweb.com/feed/",
    "https://www.bleepingcomputer.com/feed/",
    "https://venturebeat.com/feed/",
    "https://techradar.com/rss",
    "https://www.zdnet.com/news/rss.xml",
    "https://www.computerworld.com/index.rss",
    "https://news.ycombinator.com/rss",
    "https://dev.to/feed",
    "https://feeds.feedburner.com/TheRegister/headlines",
    "https://www.techmeme.com/feed.xml",
    "https://www.artificialintelligence-news.com/feed/",
    "https://machinelearningmastery.com/feed/",
    "https://www.reddit.com/r/technology/.rss",
    "https://www.reddit.com/r/MachineLearning/.rss",
    "https://www.sciencedaily.com/rss/computers_math/technology.xml",
    "https://www.eff.org/rss/updates.xml",
    "https://www.infoworld.com/index.rss",
    "https://www.techspot.com/backend.xml",
    "https://www.digitaltrends.com/news/rss",
    "https://www.tomshardware.com/feeds/all",
    "https://www.networkworld.com/index.rss",
]

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def make_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
        acks="all",
        retries=5,
    )

def stable_id(link: str, guid: str | None) -> str:
    if guid: return guid.strip()
    h = hashlib.sha1()
    h.update((link or "").encode("utf-8"))
    return h.hexdigest()

def domain_from_url(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host or ""
    except Exception:
        return ""

def is_recent(published_str: str) -> bool:
    if not published_str: return True
    try:
        pub_date = date_parser.parse(published_str)
        if pub_date.tzinfo is None:
            pub_date = pub_date.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        age = now - pub_date
        if age < timedelta(hours=MAX_AGE_HOURS) and age > timedelta(hours=-1):
            return True
        return False
    except Exception:
        return True

def fetch_from_rss(feed_url: str):
    try:
        feed = feedparser.parse(feed_url)
        out = []
        # We only check the top 5 items per cycle to keep the loop fast
        for entry in feed.entries[:5]:
            link = (entry.get("link") or "").strip()
            title = (entry.get("title") or "").strip()
            if not link or not title: continue
                
            published = entry.get("published") or entry.get("updated") or ""
            if not is_recent(published): continue

            guid = entry.get("id") or entry.get("guid") or None
            src_title = feed.feed.get("title", "Unknown")
            _id = stable_id(link, guid)
            fallback_watch_key = domain_from_url(link)

            out.append({
                "id": _id,
                "title": title,
                "url": link,
                "summary": (entry.get("summary") or entry.get("description") or "").strip(),
                "source": src_title,
                "published_at": published or iso_now(),
                "author": (entry.get("author") or "Unknown"),
                "fetched_at": iso_now(),
                "watch_key": fallback_watch_key, 
            })
        return out
    except Exception as e:
        return []

def main():
    producer = make_producer()
    
    print(f"--- LIVE NEWS STREAM STARTED ---")
    print(f"Cycling through {len(RSS_FEEDS)} sources continuously.")
    print(f"Press Ctrl+C to stop safely.\n")

    # Memory to remember what we sent SO FAR in this session.
    # This prevents sending the same article 500 times while looping.
    session_seen_ids = set()
    
    cycle_count = 0

    try:
        while True:
            for feed_url in RSS_FEEDS:
                # 1. Fetch
                articles = fetch_from_rss(feed_url)
                
                # 2. Process Immediately
                new_items_count = 0
                if articles:
                    for a in articles:
                        # ONLY send if we haven't sent it in this session yet
                        if a["id"] not in session_seen_ids:
                            producer.send(OUT_TOPIC, key=a["watch_key"], value=a)
                            session_seen_ids.add(a["id"])
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Streamed: {a['title'][:60]}...")
                            new_items_count += 1
                    
                    if new_items_count > 0:
                        producer.flush()
                
                # 3. Small politeness delay (creates the continuous stream effect)
                # Without this, you WILL get banned by the websites.
                time.sleep(INTER_REQUEST_DELAY)

            cycle_count += 1
            # Optional status update every full cycle (approx every 40 seconds)
            # print(f"--- Cycle {cycle_count} Complete. Monitoring for new updates... ---")

    except KeyboardInterrupt:
        print("\n\n[rss-fetcher] Stop command received!")
        print("[rss-fetcher] Closing Kafka connection...")
        producer.close()
        print("[rss-fetcher] Stream stopped.")
        sys.exit(0)

if __name__ == "__main__":
    main()
