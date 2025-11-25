# import os, json
# from datetime import datetime, timezone
# from typing import Dict, List, Union

# from kafka import KafkaConsumer, KafkaProducer
# from transformers import pipeline
# import spacy
# import nltk
# import requests
# from nltk.sentiment import SentimentIntensityAnalyzer
# from dotenv import load_dotenv

# load_dotenv()

# BROKER             = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC           = os.getenv("CLEAN_TOPIC", "news.cleaned")
# OUT_TOPIC          = os.getenv("ENRICHED_TOPIC", "news.enriched")
# SENTIMENT_MODEL    = os.getenv("SENTIMENT_MODEL", "sst2")
# DEVICE             = -1
# SUMMARY_WORD_LIMIT = int(os.getenv("SUMMARY_WORD_LIMIT", "100"))
# GROUP_ID           = f"enricher-{int(datetime.now().timestamp())}"

# def iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()

# def unwrap_batch(preds: Union[List, Dict]) -> List[Dict]:
#     if isinstance(preds, list) and preds and isinstance(preds[0], list): return preds[0]
#     if isinstance(preds, list): return preds
#     return [preds]

# def truncate_words(text: str, max_words: int) -> str:
#     words = (text or "").split()
#     return " ".join(words[:max_words]) if len(words) > max_words else (text or "")

# def category_from_text(text: str) -> str:
#     t = (text or "").lower()
#     if any(k in t for k in ["ai","artificial intelligence","machine learning","deep learning","neural network","chatgpt","openai","llm"]): return "AI / Machine Learning"
#     if any(k in t for k in ["cyber","hack","phishing","data breach","malware","ransomware","security flaw","zero-day"]): return "Cybersecurity"
#     if any(k in t for k in ["semiconductor","chip","nvidia","intel","amd","gpu","processor","tpu"]): return "Hardware / Semiconductors"
#     if any(k in t for k in ["startup","funding","acquisition","ipo","valuation","venture capital"]): return "Tech Business / Startups"
#     if any(k in t for k in ["social media","facebook","instagram","twitter","tiktok","reddit","youtube"]): return "Social Media / Platforms"
#     if any(k in t for k in ["cloud","aws","azure","gcp","serverless","infrastructure","saas","kubernetes"]): return "Cloud / Infrastructure"
#     if any(k in t for k in ["robot","autonomous","self-driving","tesla","drone","automation"]): return "Robotics / Automation"
#     if any(k in t for k in ["quantum","superconduct","qubit"]): return "Quantum Computing"
#     return "General Technology"

# def normalize_entities(hf_entities: List[Dict]) -> Dict[str, List[str]]:
#     out = {"ORG": [], "PERSON": [], "GPE": []}
#     for ent in hf_entities or []:
#         label = ent.get("entity_group", "")
#         text  = (ent.get("word") or "").strip()
#         if not text: continue
#         if label in ("ORG",): bucket = "ORG"
#         elif label in ("PER","PERSON"): bucket = "PERSON"
#         elif label in ("LOC",): bucket = "GPE"
#         else: continue
#         if text not in out[bucket]: out[bucket].append(text)
#     return out

# def sentiment_to_posneg_label(scores: List[Dict]) -> str:
#     if not scores: return "Negative"
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     if "POS" in lab or lab.endswith("1") or lab == "LABEL_1" or "YES" in lab: return "Positive"
#     return "Negative"

# def load_sentiment_pipe():
#     if SENTIMENT_MODEL.lower() == "finbert":
#         return pipeline("text-classification", model="ProsusAI/finbert", device=DEVICE, top_k=None)
#     else:
#         return pipeline("text-classification", model="distilbert-base-uncased-finetuned-sst-2-english", device=DEVICE, top_k=None)

# def load_ner_pipe():
#     return pipeline("token-classification", model="dslim/bert-base-NER", aggregation_strategy="simple", device=DEVICE)

# def load_spacy():
#     try: return spacy.load("en_core_web_sm")
#     except Exception:
#         from spacy.cli import download
#         download("en_core_web_sm")
#         return spacy.load("en_core_web_sm")

# def load_vader():
#     try: nltk.data.find("sentiment/vader_lexicon.zip")
#     except LookupError: nltk.download("vader_lexicon")
#     return SentimentIntensityAnalyzer()

# GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# def summarize_with_groq(title: str, original_summary: str, max_words: int) -> str:
#     base = (original_summary or "").strip()
#     if not GROQ_API_KEY: return truncate_words(base, max_words)
    
#     # IMPROVED DETAILED PROMPT FOR DASHBOARD-FRIENDLY SUMMARIES
#     prompt = f"""You are a tech news analyst creating summaries for a business intelligence dashboard.

# TASK: Summarize this tech news article in {max_words} words or less.

# REQUIREMENTS:
# - Write in third person, objective tone
# - Include: WHO (company/person), WHAT (action/event), WHY (impact/significance)
# - Mention specific numbers, dates, or metrics if available
# - Focus on business impact, market implications, or technical significance
# - Make it suitable for quick scanning in a dashboard

# DO NOT:
# - Repeat the title verbatim
# - Use vague phrases like "the article discusses" or "this is about"
# - Include opinions or speculation
# - Use promotional language or hype words
# - Start with "This article" or "The summary"

# ARTICLE TITLE: {title}

# ARTICLE CONTENT: {original_summary}

# DASHBOARD SUMMARY ({max_words} words max):"""

#     try:
#         resp = requests.post(
#             "https://api.groq.com/openai/v1/chat/completions",
#             headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
#             json={
#                 "model": "llama-3.1-8b-instant",
#                 "messages": [
#                     {
#                         "role": "system",
#                         "content": "You are a concise tech news analyst. Always respond with only the summary, no explanations or preamble."
#                     },
#                     {
#                         "role": "user",
#                         "content": prompt
#                     }
#                 ],
#                 "temperature": 0.3,
#                 "max_tokens": 220
#             },
#             timeout=15,
#         )
#         resp.raise_for_status()
#         txt = (resp.json()["choices"][0]["message"]["content"] or "").strip()
#         # Remove any "Summary:" prefix the model might add
#         import re
#         txt = re.sub(r'^(Summary|SUMMARY|Dashboard Summary)[:\s]*', '', txt).strip()
#         return truncate_words(txt, max_words) or truncate_words(base, max_words)
#     except Exception:
#         return truncate_words(base, max_words)

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
#         max_poll_records=100,
#     )

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
#         acks="all",
#         linger_ms=50,
#         retries=5,
#     )

# def main():
#     print(f"[enricher] {IN_TOPIC} → {OUT_TOPIC}")
#     consumer, producer = make_consumer(), make_producer()
#     sent_pipe, ner_pipe = load_sentiment_pipe(), load_ner_pipe()
#     _ = load_spacy(); _ = load_vader()

#     seen = set(); count = 0
#     for msg in consumer:
#         doc, in_key = msg.value, msg.key
#         title = (doc.get("title") or "").strip()
#         if not title or title.lower() in seen: continue
#         seen.add(title.lower())

#         enhanced_summary = summarize_with_groq(title, (doc.get("summary") or ""), SUMMARY_WORD_LIMIT)
#         text = (f"{title}. {enhanced_summary}")[:2000]
#         category = category_from_text(text)

#         s_scores = unwrap_batch(sent_pipe(text))
#         sentiment_label = sentiment_to_posneg_label(s_scores)

#         ner_flat  = unwrap_batch(ner_pipe(text))
#         entities  = normalize_entities(ner_flat)

#         out_key = in_key or (doc.get("watch_key") or "") or "unknown-source"
#         out = {**doc,
#             "summary": enhanced_summary,
#             "category": category,
#             "sentiment": sentiment_label,
#             "entities": entities,
#             "enriched_at": iso_now(),
#             "watch_key": out_key,
#         }
#         producer.send(OUT_TOPIC, key=out_key, value=out)
#         count += 1
#         if count % 20 == 0: print(f"[enricher] sent {count}")
#     producer.flush(); print(f"[enricher] done. total sent={count}")

# if __name__ == "__main__":
#     main()









# import os, json
# import warnings
# froim datetime import datetime, timezone
# from typing import Dict, List, Union

# warnings.simplefilter(action='ignore', category=FutureWarning)

# from kafka import KafkaConsumer, KafkaProducer
# from transformers import pipeline
# import spacy
# import nltk
# import requests
# from dotenv import load_dotenv

# load_dotenv()

# BROKER             = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC           = os.getenv("CLEAN_TOPIC", "news.cleaned")
# OUT_TOPIC          = os.getenv("ENRICHED_TOPIC", "news.enriched")
# SENTIMENT_MODEL    = os.getenv("SENTIMENT_MODEL", "sst2")
# DEVICE             = -1
# SUMMARY_WORD_LIMIT = int(os.getenv("SUMMARY_WORD_LIMIT", "100"))
# GROUP_ID           = "enricher-group-v2"

# def iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()

# def unwrap_batch(preds: Union[List, Dict]) -> List[Dict]:
#     if isinstance(preds, list) and preds and isinstance(preds[0], list): return preds[0]
#     if isinstance(preds, list): return preds
#     return [preds]

# def category_from_text(text: str) -> str:
#     t = (text or "").lower()
#     if any(k in t for k in ["ai","artificial intelligence","machine learning","gpt","llm"]): return "AI / Machine Learning"
#     if any(k in t for k in ["cyber","hack","malware","ransomware","breach"]): return "Cybersecurity"
#     if any(k in t for k in ["chip","nvidia","intel","amd","processor"]): return "Hardware"
#     if any(k in t for k in ["startup","funding","ipo","venture"]): return "Startups"
#     if any(k in t for k in ["crypto","bitcoin","blockchain","ethereum"]): return "Crypto / Blockchain"
#     if any(k in t for k in ["cloud","aws","azure","serverless"]): return "Cloud"
#     return "General Technology"

# def normalize_entities(hf_entities: List[Dict]) -> Dict[str, List[str]]:
#     out = {"ORG": [], "PERSON": [], "GPE": []}
#     for ent in hf_entities or []:
#         label = ent.get("entity_group", "")
#         text  = (ent.get("word") or "").strip()
#         if not text or len(text) < 2: continue
#         text = text.replace("##", "")
        
#         if label == "ORG": out["ORG"].append(text)
#         elif label in ("PER","PERSON"): out["PERSON"].append(text)
#         elif label == "LOC": out["GPE"].append(text)
    
#     # Deduplicate lists
#     for k in out: out[k] = list(set(out[k]))
#     return out

# def sentiment_to_posneg_label(scores: List[Dict]) -> str:
#     if not scores: return "Neutral"
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     if "POS" in lab or "LABEL_1" in lab: return "Positive"
#     return "Negative"

# def load_models():
#     print("[enricher] Loading models (this takes a moment)...")
#     # Sentiment
#     sent = pipeline("text-classification", 
#                     model="distilbert-base-uncased-finetuned-sst-2-english", 
#                     device=DEVICE, top_k=None)
#     # NER
#     ner = pipeline("token-classification", 
#                    model="dslim/bert-base-NER", 
#                    aggregation_strategy="simple", 
#                    device=DEVICE)
#     return sent, ner

# GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# def summarize_with_groq(title: str, text: str, max_words: int) -> str:
#     if not GROQ_API_KEY: return text[:500]
#     prompt = f"Summarize this tech news for a dashboard in {max_words} words. No intro. Title: {title}. Text: {text}"
#     try:
#         resp = requests.post(
#             "https://api.groq.com/openai/v1/chat/completions",
#             headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
#             json={
#                 "model": "llama-3.1-8b-instant",
#                 "messages": [{"role": "user", "content": prompt}],
#                 "max_tokens": 150
#             }, timeout=8
#         )
#         if resp.status_code == 200:
#             return resp.json()["choices"][0]["message"]["content"].strip()
#     except Exception:
#         pass
#     return text[:max_words*6]

# def main():
#     sent_pipe, ner_pipe = load_models()
#     consumer = KafkaConsumer(IN_TOPIC, bootstrap_servers=[BROKER], group_id=GROUP_ID, auto_offset_reset="earliest", value_deserializer=lambda b: json.loads(b.decode("utf-8")), consumer_timeout_ms=10000)
#     producer = KafkaProducer(bootstrap_servers=[BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8"))

#     print(f"[enricher] {IN_TOPIC} -> {OUT_TOPIC}")
#     count = 0

#     for msg in consumer:
#         doc = msg.value
#         title = (doc.get("title") or "").strip()
#         summary = (doc.get("summary") or "").strip()
        
#         # 1. Summarize
#         enhanced_summary = summarize_with_groq(title, summary, SUMMARY_WORD_LIMIT)
        
#         # 2. Prepare text for ML Models
#         # FIX: Manually truncate text to ~1500 chars (approx 300-400 tokens) 
#         # This prevents the "Sequence length is longer than 512" crash 
#         # AND avoids passing `truncation=True` to NER which causes the argument error.
#         full_text = f"{title}. {enhanced_summary}"
#         truncated_text = full_text[:1500] 

#         # 3. Categorize
#         category = category_from_text(full_text)

#         # 4. Sentiment
#         try:
#             # text-classification accepts truncation, but we used manual string slicing anyway to be safe
#             s_scores = unwrap_batch(sent_pipe(truncated_text))
#             sentiment = sentiment_to_posneg_label(s_scores)
#         except Exception:
#             sentiment = "Neutral"

#         # 5. NER (Named Entity Recognition)
#         try:
#             # NER pipeline DOES NOT accept truncation arg in __call__, so we pass the sliced string
#             ner_flat = unwrap_batch(ner_pipe(truncated_text))
#             entities = normalize_entities(ner_flat)
#         except Exception as e:
#             print(f"[enricher] NER warning: {e}")
#             entities = {}

#         # Output
#         out_key = msg.key.decode("utf-8") if msg.key else (doc.get("watch_key") or "unknown")
#         out = {**doc, "summary": enhanced_summary, "category": category, "sentiment": sentiment, "entities": entities, "enriched_at": iso_now(), "watch_key": out_key}
        
#         producer.send(OUT_TOPIC, key=out_key.encode("utf-8"), value=out)
#         count += 1
#         print(f"[enricher] Processed: {title[:40]}... [{category}]")

#     producer.flush()
#     print(f"[enricher] Done. Enriched {count} articles.")

# if __name__ == "__main__":
#     main()







# import os, json, warnings, requests
# from datetime import datetime, timezone
# from kafka import KafkaConsumer, KafkaProducer
# from transformers import pipeline
# from dotenv import load_dotenv

# load_dotenv()
# # Suppress warnings about future version changes
# warnings.simplefilter(action='ignore', category=FutureWarning)
# warnings.simplefilter(action='ignore', category=UserWarning)

# BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC = os.getenv("CLEAN_TOPIC", "news.cleaned")
# OUT_TOPIC = os.getenv("ENRICHED_TOPIC", "news.enriched")
# GROQ_API_KEY = os.getenv("GROQ_API_KEY")
# DEVICE = -1 # Use -1 for CPU, 0 for GPU (if you have CUDA/Mac M1 Metal set up)

# # --- CONFIG: ZERO-SHOT CATEGORIES ---
# # The AI will pick the best fit from this list for every article.
# CANDIDATE_LABELS = [
#     "https://spectrum.ieee.org/feed"
#     "Artificial Intelligence & Machine Learning",
#     "Cybersecurity, Hacking & Privacy",
#     "Hardware, Chips & Semiconductors",
#     "Startups, Venture Capital & Business",
#     "Consumer Electronics, Mobile & Gadgets",
#     "Social Media & Big Tech Platforms",
#     "Cloud Computing, DevOps & Software",
#     "Cryptocurrency, Blockchain & Web3",
#     "Space, Science & Robotics",
#     "Video Games & Entertainment",
#     "https://www.reddit.com/r/technology/.rss",
#     "https://www.reddit.com/r/MachineLearning/.rss",
#     "https://www.sciencedaily.com/rss/computers_math/technology.xml",
#     "https://www.eff.org/rss/updates.xml",
#     "https://www.infoworld.com/index.rss",
#     "https://www.techspot.com/backend.xml",
#     "https://www.digitaltrends.com/news/rss",
#     "https://www.tomshardware.com/feeds/all",
#     "https://www.networkworld.com/index.rss",
# ]

# def iso_now(): return datetime.now(timezone.utc).isoformat()

# def load_models():
#     print("[enricher] ⏳ Loading AI Models... (First run will download ~1.6GB for Zero-Shot)")
    
#     # 1. Sentiment Analysis (Fast)
#     print("[enricher] Loading Sentiment...")
#     sent = pipeline("text-classification", 
#                     model="distilbert-base-uncased-finetuned-sst-2-english", 
#                     top_k=None, device=DEVICE)
    
#     # 2. NER (Fast)
#     print("[enricher] Loading NER...")
#     ner = pipeline("token-classification", 
#                    model="dslim/bert-base-NER", 
#                    aggregation_strategy="simple", device=DEVICE)

#     # 3. Zero-Shot Classification (Accurate but Heavy)
#     print("[enricher] Loading Zero-Shot Classifier (facebook/bart-large-mnli)...")
#     classifier = pipeline("zero-shot-classification", 
#                           model="facebook/bart-large-mnli", 
#                           device=DEVICE)
    
#     print("[enricher] ✅ All Models Loaded.")
#     return sent, ner, classifier

# def summarize_with_groq(title, text):
#     if not GROQ_API_KEY: return text[:300]
#     try:
#         resp = requests.post("https://api.groq.com/openai/v1/chat/completions",
#             headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
#             json={"model": "llama-3.1-8b-instant", "messages": [{"role": "user", "content": f"Summarize for dashboard (max 60 words): {title}. {text}"}]}, timeout=10)
#         if resp.status_code == 200: return resp.json()["choices"][0]["message"]["content"]
#     except: pass
#     return text[:300]

# def get_accurate_category(text, classifier_pipe):
#     """
#     Uses Zero-Shot Classification to categorize text into CANDIDATE_LABELS.
#     """
#     try:
#         # Truncate to 1024 chars to keep it reasonably fast and prevent crashes
#         safe_text = text[:1024] 
#         result = classifier_pipe(safe_text, CANDIDATE_LABELS)
#         # result['labels'] is sorted by score (highest first)
#         return result['labels'][0]
#     except Exception as e:
#         print(f"[enricher] Category Error: {e}")
#         return "General Technology"

# def main():
#     sent_pipe, ner_pipe, zero_shot_pipe = load_models()
    
#     consumer = KafkaConsumer(
#         IN_TOPIC, bootstrap_servers=[BROKER], group_id="enricher-zeroshot-v1",
#         auto_offset_reset="earliest", value_deserializer=lambda b: json.loads(b.decode("utf-8"))
#     )
#     producer = KafkaProducer(bootstrap_servers=[BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8"))

#     print(f"[enricher] 🚀 Zero-Shot Enricher Active. Listening on {IN_TOPIC}...")

#     for msg in consumer:
#         doc = msg.value
#         title = doc.get("title", "")
#         summary_text = doc.get("summary", "")
        
#         # 1. Summarize (Groq)
#         enhanced_summary = summarize_with_groq(title, summary_text)
        
#         # Prepare text for AI (Title + Summary contains the most info)
#         full_text = f"{title}. {enhanced_summary}"
        
#         # 2. Categorize (Zero-Shot)
#         # This is the "Heavy" step.
#         category = get_accurate_category(full_text, zero_shot_pipe)
        
#         # 3. Sentiment
#         try: 
#             # Truncate to 512 tokens approx
#             sent_res = sent_pipe(full_text[:1500]) 
#             sent_label = sent_res[0][0]['label'] # POSITIVE / NEGATIVE
#         except: sent_label = "Neutral"
        
#         # 4. NER
#         try: 
#             ents = ner_pipe(full_text[:1500])
#             entities = {"ORG": list(set(e['word'] for e in ents if e['entity_group'] == 'ORG'))}
#         except: entities = {}

#         # Output
#         out = {
#             **doc, 
#             "summary": enhanced_summary, 
#             "sentiment": sent_label, 
#             "category": category, 
#             "entities": entities, 
#             "enriched_at": iso_now()
#         }
        
#         producer.send(OUT_TOPIC, key=msg.key, value=out)
#         print(f"[enricher] {category[:20]}... | {title[:40]}...")

# if __name__ == "__main__":
#     main()












import os, json, warnings, requests
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline
from dotenv import load_dotenv

load_dotenv()
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC = os.getenv("CLEAN_TOPIC", "news.cleaned")
OUT_TOPIC = os.getenv("ENRICHED_TOPIC", "news.enriched")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
DEVICE = -1 

# --- CONFIG: ZERO-SHOT CATEGORIES ---
CANDIDATE_LABELS = [
    "Artificial Intelligence & Machine Learning",
    "Cybersecurity, Hacking & Privacy",
    "Hardware, Chips & Semiconductors",
    "Startups, Venture Capital & Business",
    "Consumer Electronics, Mobile & Gadgets",
    "Social Media & Big Tech Platforms",
    "Cloud Computing, DevOps & Software",
    "Cryptocurrency, Blockchain & Web3",
    "Space, Science & Robotics",
    "Video Games & Entertainment"
]

def iso_now(): return datetime.now(timezone.utc).isoformat()

def load_models():
    print("[enricher] ⏳ Loading AI Models... (First run will download ~1.6GB for Zero-Shot)")
    
    # 1. Sentiment Analysis (Fast)
    print("[enricher] Loading Sentiment...")
    sent = pipeline("text-classification", 
                    model="distilbert-base-uncased-finetuned-sst-2-english", 
                    top_k=None, device=DEVICE)
    
    # 2. NER (Fast)
    print("[enricher] Loading NER...")
    ner = pipeline("token-classification", 
                   model="dslim/bert-base-NER", 
                   aggregation_strategy="simple", device=DEVICE)

    # 3. Zero-Shot Classification (Accurate but Heavy)
    print("[enricher] Loading Zero-Shot Classifier (facebook/bart-large-mnli)...")
    classifier = pipeline("zero-shot-classification", 
                          model="facebook/bart-large-mnli", 
                          device=DEVICE)
    
    print("[enricher] All Models Loaded.")
    return sent, ner, classifier

def summarize_with_groq(title, text):
    if not GROQ_API_KEY: return text[:300]
    try:
        resp = requests.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
            json={"model": "llama-3.1-8b-instant", "messages": [{"role": "user", "content": f"Summarize for dashboard (max 60 words): {title}. {text}"}]}, timeout=10)
        if resp.status_code == 200: return resp.json()["choices"][0]["message"]["content"]
    except: pass
    return text[:300]

def get_accurate_category(text, classifier_pipe):
    try:
        safe_text = text[:1024] 
        result = classifier_pipe(safe_text, CANDIDATE_LABELS)
        return result['labels'][0]
    except Exception as e:
        print(f"[enricher] Category Error: {e}")
        return "General Technology"

def main():
    sent_pipe, ner_pipe, zero_shot_pipe = load_models()
    
    consumer = KafkaConsumer(
        IN_TOPIC, bootstrap_servers=[BROKER], group_id="enricher-zeroshot-v2",
        auto_offset_reset="earliest", value_deserializer=lambda b: json.loads(b.decode("utf-8"))
    )
    producer = KafkaProducer(bootstrap_servers=[BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    print(f"[enricher] Zero-Shot Enricher Active. Listening on {IN_TOPIC}...")

    for msg in consumer:
        doc = msg.value
        title = doc.get("title", "")
        summary_text = doc.get("summary", "")
        
        # 1. Summarize
        enhanced_summary = summarize_with_groq(title, summary_text)
        full_text = f"{title}. {enhanced_summary}"
        
        # 2. Categorize (Zero-Shot)
        category = get_accurate_category(full_text, zero_shot_pipe)
        
        # 3. Sentiment & Confidence Score
        try: 
            # Truncate to avoid crash
            sent_res = sent_pipe(full_text[:1500]) 
            # Output format is like: [[{'label': 'POSITIVE', 'score': 0.998}, ...]]
            top_result = sent_res[0][0]
            sent_label = top_result['label'] 
            sent_score = top_result['score'] # This is the "Confidence" (0.0 to 1.0)
        except: 
            sent_label = "Neutral"
            sent_score = 0.0
        
        # 4. NER
        try: 
            ents = ner_pipe(full_text[:1500])
            entities = {"ORG": list(set(e['word'] for e in ents if e['entity_group'] == 'ORG'))}
        except: entities = {}

        # Output
        out = {
            **doc, 
            "summary": enhanced_summary, 
            "sentiment": sent_label, 
            "sentiment_conf": sent_score, # Save confidence to JSON too
            "category": category, 
            "entities": entities, 
            "enriched_at": iso_now()
        }
        
        producer.send(OUT_TOPIC, key=msg.key, value=out)
        
        # PRINTING THE CONFIDENCE IN TERMINAL
        print(f"[enricher] {category[:15]}... | {sent_label} ({sent_score:.2%}) | {title[:30]}...")

if __name__ == "__main__":
    main()
