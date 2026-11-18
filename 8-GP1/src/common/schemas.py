"""Message schemas shared by all components (see contract.md, shared data contract)."""

from datetime import datetime, timezone
from typing import Any

TOPIC1_FIELDS = ("message_id", "text", "source", "event_time")
TOPIC2_FIELDS = ("entity", "entity_type", "count", "window_end", "run_id")
TOPIC3_FIELDS = ("class_name", "count", "window_end", "run_id")

AG_NEWS_LABELS = {0: "World", 1: "Sports", 2: "Business", 3: "Sci/Tech"}
MANUAL_ONLY_LABEL = "None of these"


def utc_iso(moment: datetime) -> str:
    """Format a datetime as a UTC ISO-8601 string ending in Z."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def build_topic1_message(message_id: str, text: str, source: str, event_time: datetime) -> dict[str, Any]:
    """Create a validated topic1 message."""
    message = {
        "message_id": str(message_id),
        "text": text.strip(),
        "source": source,
        "event_time": utc_iso(event_time),
    }
    validate_topic1_message(message)
    return message


def validate_topic1_message(message: Any) -> None:
    """Raise ValueError when a message does not follow the topic1 contract."""
    if not isinstance(message, dict):
        raise ValueError("topic1 message must be a JSON object")
    missing = [field for field in TOPIC1_FIELDS if field not in message]
    if missing:
        raise ValueError(f"topic1 message is missing fields: {missing}")
    for field in TOPIC1_FIELDS:
        if not isinstance(message[field], str):
            raise ValueError(f"topic1 field '{field}' must be a string")
    if not message["message_id"].strip():
        raise ValueError("topic1 message_id must not be empty")
    if not message["text"].strip():
        raise ValueError("topic1 text must not be empty")
    try:
        datetime.fromisoformat(message["event_time"].replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"topic1 event_time is not ISO-8601: {message['event_time']}") from error
