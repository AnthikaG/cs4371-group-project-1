"""Validate topic1 records and extract normalized named entities with spaCy."""

import json
import unicodedata
from datetime import datetime, timedelta

from src.common.schemas import validate_topic1_message

_model = None


def load_model():
    """Reuse one trained English pipeline in each Python worker."""
    global _model
    if _model is None:
        import spacy

        _model = spacy.load("en_core_web_sm", disable=["tagger", "parser", "lemmatizer", "attribute_ruler"])
    return _model


def parse_text(value) -> str | None:
    """Return valid message text, or None for malformed or empty input."""
    try:
        if isinstance(value, (bytes, bytearray)):
            value = bytes(value).decode("utf-8")
        message = json.loads(value)
        validate_topic1_message(message)
        timestamp = datetime.fromisoformat(message["event_time"].replace("Z", "+00:00"))
        if timestamp.utcoffset() != timedelta(0) or not message["source"].strip():
            return None
        return message["text"].strip()
    except (ValueError, TypeError, OverflowError):
        return None


def normalize_entity(text: str) -> str:
    """Merge casing, compatibility characters and extra whitespace."""
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def extract_entities(text: str | None) -> list[tuple[str, str]]:
    """Return each recognized mention and type, including repeated mentions."""
    if not text:
        return []
    return [(normalize_entity(entity.text), entity.label_) for entity in load_model()(text).ents
            if normalize_entity(entity.text)]
