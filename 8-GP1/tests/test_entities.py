import json
from types import SimpleNamespace

import pytest

from src.streaming import ner
from src.streaming.entity_stream import get_run_id, parse_args


def message(**changes):
    return json.dumps({"message_id": "1", "text": "Apple", "source": "test",
                       "event_time": "2026-11-13T00:00:00Z", **changes})


@pytest.mark.parametrize("value", [
    "not json", "[]", "{}", "null", None, b"\xff", message(text=" "),
    message(text=123), message(message_id=""), message(source=""),
    message(event_time="invalid"), message(event_time="2026-11-13T00:00:00"),
])
def test_bad_messages_are_filtered(value):
    assert ner.parse_text(value) is None


def test_valid_utf8_message():
    assert ner.parse_text(message(text="  Apple  ").encode("utf-8")) == "Apple"


def test_entity_normalization():
    assert ner.normalize_entity("  New\tYork ") == "new york"
    assert ner.normalize_entity("ＡＰＰＬＥ") == "apple"


def test_each_entity_mention_is_preserved(monkeypatch):
    mentions = [SimpleNamespace(text="Apple", label_="ORG"),
                SimpleNamespace(text="APPLE", label_="ORG"),
                SimpleNamespace(text="Jordan", label_="PERSON")]
    monkeypatch.setattr(ner, "load_model", lambda: lambda text: SimpleNamespace(ents=mentions))
    assert ner.extract_entities("fixture") == [("apple", "ORG"), ("apple", "ORG"), ("jordan", "PERSON")]
    assert ner.extract_entities(None) == []


def test_run_id_survives_restart_and_rejects_changed_settings(tmp_path):
    configuration = {"input_topic": "topic1"}
    run_id = get_run_id(tmp_path, configuration, "test-run")
    checkpoint = tmp_path / "spark"
    checkpoint.mkdir()
    (checkpoint / "metadata").write_text("{}")
    assert get_run_id(tmp_path, configuration, None) == run_id
    with pytest.raises(ValueError, match="run ID"):
        get_run_id(tmp_path, configuration, "different")
    with pytest.raises(ValueError, match="Settings changed"):
        get_run_id(tmp_path, {"input_topic": "other"}, None)


def test_incomplete_checkpoint_is_rejected(tmp_path):
    get_run_id(tmp_path, {}, "test-run")
    with pytest.raises(ValueError, match="Spark state is missing"):
        get_run_id(tmp_path, {}, None)


@pytest.mark.parametrize("arguments", [
    ["--trigger-seconds", "0"], ["--max-offsets-per-trigger", "0"],
    ["--run-id", " "], ["--input-topic", "same", "--output-topic", "same"],
])
def test_invalid_arguments(arguments):
    with pytest.raises(SystemExit):
        parse_args(arguments)
