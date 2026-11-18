"""Stream live text into Kafka topic1.

Sources:
  bluesky    public Bluesky posts over the Jetstream WebSocket (no key needed)
  hackernews new Hacker News stories and comments by polling the Firebase API (no key needed)
  replay     re-send messages from a JSONL file (for example data/sample/topic1_sample.jsonl)

Every message sent to Kafka follows the topic1 contract:
  {"message_id": str, "text": str, "source": str, "event_time": UTC ISO-8601 str}

Examples:
  python -m src.ingestion.producer --source bluesky --capture-file outputs/topic1_capture.jsonl
  python -m src.ingestion.producer --source hackernews --max-messages 50 --dry-run
  python -m src.ingestion.producer --source replay --replay-file data/sample/topic1_sample.jsonl
"""

import argparse
import html
import json
import logging
import random
import re
import signal
import sys
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from config.settings import get_settings
from src.common.schemas import build_topic1_message, validate_topic1_message

LOGGER = logging.getLogger("producer")

JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
HN_BASE_URL = "https://hacker-news.firebaseio.com/v0"
MAX_BACKOFF_SECONDS = 60
SEEN_ID_CAPACITY = 200_000
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"\s+")


class StopRequested:
    """Flag flipped by ctrl+c or SIGTERM so loops can exit cleanly."""

    def __init__(self) -> None:
        self.stopped = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame) -> None:
        LOGGER.info("stop requested, finishing up")
        self.stopped = True


class SeenIds:
    """Bounded memory of message ids already sent, to drop duplicates."""

    def __init__(self, capacity: int = SEEN_ID_CAPACITY) -> None:
        self.capacity = capacity
        self._ids: OrderedDict[str, None] = OrderedDict()

    def add_if_new(self, message_id: str) -> bool:
        if message_id in self._ids:
            return False
        self._ids[message_id] = None
        if len(self._ids) > self.capacity:
            self._ids.popitem(last=False)
        return True


def clean_text(raw_text: Optional[str]) -> str:
    """Remove html tags and entities and collapse whitespace."""
    if not raw_text:
        return ""
    # paragraph tags carry meaning in hn comments, keep them as spaces
    without_tags = HTML_TAG_PATTERN.sub(" ", raw_text)
    return WHITESPACE_PATTERN.sub(" ", html.unescape(without_tags)).strip()


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with jitter, capped."""
    return min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)


# ---------------------------------------------------------------- sources

def parse_jetstream_event(event: dict[str, Any], language: Optional[str]) -> Optional[dict[str, Any]]:
    """Turn one Jetstream event into a topic1 message, or None if it should be skipped."""
    if event.get("kind") != "commit":
        return None
    commit = event.get("commit") or {}
    if commit.get("operation") != "create" or commit.get("collection") != "app.bsky.feed.post":
        return None
    record = commit.get("record") or {}
    if language and language not in (record.get("langs") or []):
        return None
    text = clean_text(record.get("text"))
    if not text:
        return None
    did = event.get("did")
    rkey = commit.get("rkey")
    if not did or not rkey:
        return None
    event_time = datetime.fromtimestamp(event.get("time_us", time.time() * 1_000_000) / 1_000_000, tz=timezone.utc)
    return build_topic1_message(
        message_id=f"at://{did}/app.bsky.feed.post/{rkey}",
        text=text,
        source="bluesky_jetstream",
        event_time=event_time,
    )


def stream_bluesky(stop: StopRequested, language: Optional[str], sample_rate: float) -> Iterator[dict[str, Any]]:
    """Yield posts from Jetstream, reconnecting with backoff and resuming from the last cursor."""
    import websocket

    cursor: Optional[int] = None
    attempt = 0
    while not stop.stopped:
        url = JETSTREAM_URL if cursor is None else f"{JETSTREAM_URL}&cursor={cursor}"
        try:
            LOGGER.info("connecting to jetstream")
            connection = websocket.create_connection(url, timeout=30)
            attempt = 0
            while not stop.stopped:
                event = json.loads(connection.recv())
                cursor = event.get("time_us", cursor)
                message = parse_jetstream_event(event, language)
                # firehose is very high volume, keep a random sample
                if message and random.random() < sample_rate:
                    yield message
            connection.close()
        except Exception as error:  # network errors of many kinds, always retry
            wait = backoff_seconds(attempt)
            attempt += 1
            LOGGER.warning("jetstream error (%s), reconnecting in %.1fs", error, wait)
            time.sleep(wait)


def parse_hn_item(item: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Turn one Hacker News item into a topic1 message, or None if it should be skipped."""
    if not item or item.get("deleted") or item.get("dead"):
        return None
    if item.get("type") not in ("story", "comment"):
        return None
    text = clean_text(" ".join(part for part in (item.get("title"), item.get("text")) if part))
    if not text:
        return None
    return build_topic1_message(
        message_id=f"hn-{item['id']}",
        text=text,
        source="hacker_news",
        event_time=datetime.fromtimestamp(item.get("time", time.time()), tz=timezone.utc),
    )


def fetch_json_with_retry(session, url: str, stop: StopRequested, retries: int = 5) -> Any:
    for attempt in range(retries):
        if stop.stopped:
            return None
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as error:
            wait = backoff_seconds(attempt)
            LOGGER.warning("request failed for %s (%s), retrying in %.1fs", url, error, wait)
            time.sleep(wait)
    LOGGER.error("giving up on %s", url)
    return None


def stream_hackernews(stop: StopRequested, poll_interval: float, backfill: int) -> Iterator[dict[str, Any]]:
    """Poll maxitem and yield every new story and comment."""
    import requests

    session = requests.Session()
    last_seen: Optional[int] = None
    while not stop.stopped:
        max_item = fetch_json_with_retry(session, f"{HN_BASE_URL}/maxitem.json", stop)
        if isinstance(max_item, int):
            if last_seen is None:
                last_seen = max_item - backfill
            for item_id in range(last_seen + 1, max_item + 1):
                if stop.stopped:
                    return
                item = fetch_json_with_retry(session, f"{HN_BASE_URL}/item/{item_id}.json", stop)
                message = parse_hn_item(item)
                if message:
                    yield message
            last_seen = max(last_seen, max_item)
        time.sleep(poll_interval)


def stream_replay(replay_file: Path, stop: StopRequested, delay: float) -> Iterator[dict[str, Any]]:
    """Yield valid messages from a JSONL file."""
    with replay_file.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if stop.stopped:
                return
            if not line.strip():
                continue
            try:
                message = json.loads(line)
                validate_topic1_message(message)
            except ValueError as error:
                LOGGER.warning("skipping line %d of %s: %s", line_number, replay_file, error)
                continue
            yield message
            if delay:
                time.sleep(delay)


# ---------------------------------------------------------------- sinks

class KafkaSink:
    """Send messages to Kafka with the message id as the key."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        from confluent_kafka import Producer

        self.topic = topic
        self.failed = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "client.id": "cs4371-topic1-producer",
        })
        # fail fast when kafka is not reachable instead of buffering forever
        try:
            self.producer.list_topics(timeout=10)
        except Exception as error:
            raise SystemExit(
                f"cannot reach Kafka at {bootstrap_servers} ({error}). "
                "Is the Kafka container running? Try: docker compose up -d kafka"
            )

    def _on_delivery(self, error, message) -> None:
        if error is not None:
            self.failed += 1
            LOGGER.error("delivery failed: %s", error)

    def send(self, message: dict[str, Any]) -> None:
        payload = json.dumps(message, ensure_ascii=False).encode("utf-8")
        while True:
            try:
                self.producer.produce(self.topic, key=message["message_id"].encode("utf-8"),
                                      value=payload, on_delivery=self._on_delivery)
                break
            except BufferError:
                # local queue full, let it drain
                self.producer.poll(1)
        self.producer.poll(0)

    def close(self) -> None:
        remaining = self.producer.flush(30)
        if remaining:
            LOGGER.error("%d messages were not delivered before shutdown", remaining)


class PrintSink:
    """Dry-run sink that prints messages instead of sending them."""

    def send(self, message: dict[str, Any]) -> None:
        print(json.dumps(message, ensure_ascii=False), flush=True)

    def close(self) -> None:
        pass


# ---------------------------------------------------------------- main

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Stream live text into Kafka topic1.")
    parser.add_argument("--source", choices=("bluesky", "hackernews", "replay"), default="bluesky")
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_input_topic)
    parser.add_argument("--capture-file", type=Path,
                        help="also append every sent message to this JSONL file (for Part 2)")
    parser.add_argument("--replay-file", type=Path, help="JSONL file used with --source replay")
    parser.add_argument("--replay-delay", type=float, default=0.0, help="seconds between replayed messages")
    parser.add_argument("--max-messages", type=int, default=0, help="stop after this many messages (0 = no limit)")
    parser.add_argument("--duration-minutes", type=float, default=0.0, help="stop after this long (0 = no limit)")
    parser.add_argument("--language", default="en", help="bluesky only: keep posts tagged with this language ('' = all)")
    parser.add_argument("--sample-rate", type=float, default=0.2,
                        help="bluesky only: fraction of posts to keep, between 0 and 1")
    parser.add_argument("--poll-interval", type=float, default=10.0, help="hackernews only: seconds between polls")
    parser.add_argument("--backfill", type=int, default=100,
                        help="hackernews only: also fetch this many items from just before start")
    parser.add_argument("--dry-run", action="store_true", help="print messages instead of sending to Kafka")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    if args.source == "replay" and not args.replay_file:
        parser.error("--source replay needs --replay-file")
    if not 0 < args.sample_rate <= 1:
        parser.error("--sample-rate must be in (0, 1]")
    return args


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                        stream=sys.stderr)
    stop = StopRequested()

    if args.source == "bluesky":
        messages = stream_bluesky(stop, args.language or None, args.sample_rate)
    elif args.source == "hackernews":
        messages = stream_hackernews(stop, args.poll_interval, args.backfill)
    else:
        messages = stream_replay(args.replay_file, stop, args.replay_delay)

    sink = PrintSink() if args.dry_run else KafkaSink(args.bootstrap_servers, args.topic)
    capture_handle = None
    if args.capture_file:
        args.capture_file.parent.mkdir(parents=True, exist_ok=True)
        capture_handle = args.capture_file.open("a", encoding="utf-8")

    seen_ids = SeenIds()
    sent = 0
    duplicates = 0
    started = time.monotonic()
    deadline = started + args.duration_minutes * 60 if args.duration_minutes else None
    LOGGER.info("source=%s topic=%s dry_run=%s", args.source, args.topic, args.dry_run)
    try:
        for message in messages:
            if not seen_ids.add_if_new(message["message_id"]):
                duplicates += 1
                continue
            sink.send(message)
            if capture_handle:
                capture_handle.write(json.dumps(message, ensure_ascii=False) + "\n")
                capture_handle.flush()
            sent += 1
            if sent % 100 == 0:
                LOGGER.info("sent %d messages (%d duplicates skipped)", sent, duplicates)
            if args.max_messages and sent >= args.max_messages:
                break
            if deadline and time.monotonic() >= deadline:
                break
            if stop.stopped:
                break
    finally:
        sink.close()
        if capture_handle:
            capture_handle.close()
        elapsed = time.monotonic() - started
        failed = getattr(sink, "failed", 0)
        LOGGER.info("done: sent=%d duplicates_skipped=%d delivery_failures=%d elapsed=%.0fs",
                    sent, duplicates, failed, elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
