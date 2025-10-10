import hashlib
import re
from bs4 import BeautifulSoup

def strip_html(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html5lib")
    # get visible text
    cleaned = soup.get_text(separator=" ", strip=True)
    # collapse whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def stable_id(url: str) -> str:
    # canonical stable id from URL
    return hashlib.sha256((url or "").encode("utf-8")).hexdigest()
