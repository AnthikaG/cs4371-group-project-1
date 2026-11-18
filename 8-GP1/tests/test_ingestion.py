import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.common.schemas import build_topic1_message, validate_topic1_message
from src.ingestion.count_messages import count_messages
from src.ingestion.producer import SeenIds, clean_text, parse_hn_item, parse_jetstream_event

SAMPLE_FILE = Path(__file__).resolve().parents[1] / "data" / "sample" / "topic1_sample.jsonl"


def jetstream_event(text="Hello from Austin", langs=("en",), operation="create"):
    return {
        "did": "did:plc:abc123",
        "time_us": 1_763_000_000_000_000,
        "kind": "commit",
        "commit": {
            "operation": operation,
            "collection": "app.bsky.feed.post",
            "rkey": "3kxyz",
            "record": {"text": text, "langs": list(langs)},
        },
    }


def test_build_message_has_contract_fields():
    message = build_topic1_message("id-1", "  some text  ", "test", datetime(2026, 11, 13, tzinfo=timezone.utc))
    assert set(message) == {"message_id", "text", "source", "event_time"}
    assert message["text"] == "some text"
    assert message["event_time"].endswith("Z")


def test_empty_text_is_rejected():
    with pytest.raises(ValueError):
        build_topic1_message("id-1", "   ", "test", datetime.now(timezone.utc))


def test_missing_field_is_rejected():
    with pytest.raises(ValueError):
        validate_topic1_message({"message_id": "1", "text": "hi", "source": "x"})


def test_clean_text_strips_html():
    assert clean_text("I&#x27;d say <i>yes</i><p>second  para") == "I'd say yes second para"


def test_jetstream_post_is_parsed_with_stable_id():
    first = parse_jetstream_event(jetstream_event(), "en")
    second = parse_jetstream_event(jetstream_event(), "en")
    assert first["message_id"] == "at://did:plc:abc123/app.bsky.feed.post/3kxyz"
    assert first["message_id"] == second["message_id"]
    assert first["source"] == "bluesky_jetstream"


def test_jetstream_filters_language_deletes_and_empty():
    assert parse_jetstream_event(jetstream_event(langs=("ja",)), "en") is None
    assert parse_jetstream_event(jetstream_event(operation="delete"), "en") is None
    assert parse_jetstream_event(jetstream_event(text="   "), "en") is None
    assert parse_jetstream_event({"kind": "identity"}, "en") is None


def test_hn_comment_is_parsed_and_dead_skipped():
    message = parse_hn_item({"id": 42, "type": "comment", "text": "Rust &amp; Go", "time": 1_763_000_000})
    assert message["message_id"] == "hn-42"
    assert message["text"] == "Rust & Go"
    assert parse_hn_item({"id": 43, "type": "comment", "dead": True, "text": "x", "time": 1}) is None
    assert parse_hn_item({"id": 44, "type": "job", "title": "hiring", "time": 1}) is None
    assert parse_hn_item(None) is None


def test_seen_ids_drops_duplicates():
    seen = SeenIds(capacity=2)
    assert seen.add_if_new("a")
    assert not seen.add_if_new("a")


def test_sample_fixture_is_valid():
    lines = [json.loads(line) for line in SAMPLE_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert lines
    for message in lines:
        validate_topic1_message(message)


def test_count_messages_removes_duplicates_and_empty(tmp_path):
    rows = [
        {"message_id": "1", "text": "alpha", "source": "s", "event_time": "2026-11-13T00:00:00Z"},
        {"message_id": "1", "text": "alpha", "source": "s", "event_time": "2026-11-13T00:00:00Z"},
        {"message_id": "2", "text": "alpha", "source": "s", "event_time": "2026-11-13T00:00:01Z"},
        {"message_id": "3", "text": " ", "source": "s", "event_time": "2026-11-13T00:00:02Z"},
        {"message_id": "4", "text": "beta", "source": "s", "event_time": "2026-11-13T00:00:03Z"},
    ]
    capture = tmp_path / "capture.jsonl"
    capture.write_text("\n".join(json.dumps(row) for row in rows) + "\nnot json\n", encoding="utf-8")
    counts = count_messages([capture])
    assert counts["raw_messages"] == 6
    assert counts["duplicate_message_ids"] == 1
    assert counts["duplicate_texts"] == 1
    assert counts["empty_text_messages"] == 1
    assert counts["invalid_messages"] == 1
    assert counts["cleaned_messages"] == 2
