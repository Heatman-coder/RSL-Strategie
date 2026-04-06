import re
import unicodedata
from typing import Any


_CORPORATE_SUFFIX_TOKENS = {
    "a",
    "ab",
    "ag",
    "an",
    "asa",
    "as",
    "bhd",
    "co",
    "company",
    "corp",
    "corporation",
    "group",
    "holding",
    "holdings",
    "inc",
    "incorporated",
    "kgaa",
    "limited",
    "ltd",
    "nv",
    "oy",
    "oyj",
    "plc",
    "pte",
    "public",
    "sa",
    "se",
    "spa",
    "the",
}


def normalize_name_for_dedup(name: Any) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("&", " and ")
    text = re.sub(r"\bs\.?\s*a\.?\b", " sa ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bn\.?\s*v\.?\b", " nv ", text, flags=re.IGNORECASE)
    text = re.sub(r"\ba\s*/\s*s\s*a\b", " asa ", text, flags=re.IGNORECASE)
    text = re.sub(r"\ba\s*/\s*s\b", " as ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bo\.?\s*y\.?\s*j\.?\b", " oyj ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bo\.?\s*y\.?\b", " oy ", text, flags=re.IGNORECASE)
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text.lower())
    tokens = [tok for tok in text.split() if tok and tok not in _CORPORATE_SUFFIX_TOKENS]
    return "".join(tokens)
