import os, json
from datetime import datetime, timezone
from typing import Dict, List, Union

from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline
import spacy
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# -------------------- CONFIG --------------------
BROKER           = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC         = os.getenv("CLEAN_TOPIC", "news.cleaned")
OUT_TOPIC        = os.getenv("ENRICHED_TOPIC", "news.enriched")
SENTIMENT_MODEL  = os.getenv("SENTIMENT_MODEL", "sst2")  # "sst2" or "finbert"
DEVICE           = -1  # CPU

# -------------------- UTILS ---------------------
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def unwrap_batch(preds: Union[List, Dict]) -> List[Dict]:
    """
    Normalize HF pipeline outputs:
      - text-classification with top_k=None -> [[{label, score}, ...]]
      - text-classification default         -> [{label, score}, ...]
      - token-classification (ner)          -> [{...}, ...]
    """
    if isinstance(preds, list) and preds and isinstance(preds[0], list):
        return preds[0]
    if isinstance(preds, list):
        return preds
    return [preds]

def category_from_scores(scores: List[Dict]) -> str:
    if not scores:
        return "general"
    best = max(scores, key=lambda x: x.get("score", 0.0))
    label = (best.get("label") or "").lower()
    mapping = {"world": "world", "sports": "sports", "business": "business", "sci/tech": "technology"}
    return mapping.get(label, label or "general")

def sentiment_to_score_sst2(scores: List[Dict]) -> float:
    if not scores:
        return 0.0
    best = max(scores, key=lambda x: x.get("score", 0.0))
    lab = (best.get("label") or "").upper()
    s = float(best.get("score") or 0.0)
    return s if lab == "POSITIVE" else -s

def sentiment_to_score_finbert(scores: List[Dict]) -> float:
    if not scores:
        return 0.0
    best = max(scores, key=lambda x: x.get("score", 0.0))
    lab = (best.get("label") or "").upper()
    s = float(best.get("score") or 0.0)
    if lab == "POSITIVE":
        return s
    if lab == "NEGATIVE":
        return -s
    return 0.0

def normalize_entities(hf_entities: List[Dict]) -> Dict[str, List[str]]:
    out = {"ORG": [], "PERSON": [], "GPE": []}
    for ent in hf_entities or []:
        label = ent.get("entity_group", "")
        text  = (ent.get("word") or "").strip()
        if not text:
            continue
        if label in ("ORG",):
            bucket = "ORG"
        elif label in ("PER", "PERSON"):
            bucket = "PERSON"
        elif label in ("LOC",):
            bucket = "GPE"
        else:
            continue
        if text not in out[bucket]:
            out[bucket].append(text)
    return out

# -------------------- LOADERS -------------------
def load_category_pipe():
    # DistilBERT AG News classifier
    return pipeline(
        "text-classification",
        model="textattack/distilbert-base-uncased-ag-news",
        device=DEVICE,
        top_k=None      # replaces deprecated return_all_scores=True
    )

def load_sentiment_pipe():
    if SENTIMENT_MODEL.lower() == "finbert":
        return pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            device=DEVICE,
            top_k=None
        )
    else:
        return pipeline(
            "text-classification",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            device=DEVICE,
            top_k=None
        )

def load_ner_pipe():
    # Note: DO NOT pass truncation here; pipeline will handle chunking
    return pipeline(
        "token-classification",
        model="dslim/bert-base-NER",
        aggregation_strategy="simple",
        device=DEVICE
    )

def load_spacy():
    try:
        return spacy.load("en_core_web_sm")
    except Exception:
        from spacy.cli import download
        download("en_core_web_sm")
        return spacy.load("en_core_web_sm")

def load_vader():
    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError:
        nltk.download("vader_lexicon")
    return SentimentIntensityAnalyzer()

# -------------------- KAFKA ---------------------
def make_consumer():
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BROKER],
        group_id="enricher-service",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=30000,
        max_poll_records=100,
    )

def make_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
        retries=5,
    )

# -------------------- MAIN ----------------------
def main():
    print(f"[enricher] broker={BROKER} {IN_TOPIC} → {OUT_TOPIC} (sentiment={SENTIMENT_MODEL})")

    consumer = make_consumer()
    producer = make_producer()

    cat_pipe = load_category_pipe()
    sent_pipe = load_sentiment_pipe()
    ner_pipe  = load_ner_pipe()
    _nlp      = load_spacy()           # reserved if you want extra cleaning later
    _vader    = load_vader()           # optional blend, not used by default

    count = 0
    for msg in consumer:
        doc = msg.value
        title   = (doc.get("title") or "").strip()
        summary = (doc.get("summary") or "").strip()
        # keep input short-ish to speed up CPU inference
        text = (f"{title}. {summary}").strip()[:4000]

        # CATEGORY
        cat_raw   = cat_pipe(text)          # no truncation kwarg
        cat_scores= unwrap_batch(cat_raw)
        category  = category_from_scores(cat_scores)

        # SENTIMENT
        s_raw     = sent_pipe(text)
        s_scores  = unwrap_batch(s_raw)
        if SENTIMENT_MODEL.lower() == "finbert":
            sentiment = round(sentiment_to_score_finbert(s_scores), 4)
        else:
            sentiment = round(sentiment_to_score_sst2(s_scores), 4)

        # NER
        ner_raw   = ner_pipe(text)          # no truncation kwarg
        ner_flat  = unwrap_batch(ner_raw)
        entities  = normalize_entities(ner_flat)

        out = {
            **doc,
            "category": category,
            "sentiment": sentiment,     # -1..1
            "entities": entities,       # {ORG:[], PERSON:[], GPE:[]}
            "enriched_at": iso_now(),
        }

        producer.send(OUT_TOPIC, value=out)
        count += 1
        if count % 20 == 0:
            print(f"[enricher] sent {count} → {OUT_TOPIC}")

    producer.flush()
    print(f"[enricher] done. total sent={count}")

if __name__ == "__main__":
    main()
