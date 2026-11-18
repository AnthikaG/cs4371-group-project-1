"""Report raw and cleaned message counts for a captured topic1 JSONL file.

Example:
  python -m src.ingestion.count_messages outputs/topic1_capture.jsonl --output evidence/ingestion_counts.json
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from src.common.schemas import validate_topic1_message

REQUIRED_MESSAGES = 1000


def count_messages(paths: list[Path]) -> dict:
    raw_lines = 0
    invalid = 0
    empty_text = 0
    seen_ids: set[str] = set()
    seen_texts: set[str] = set()
    duplicate_ids = 0
    duplicate_texts = 0
    sources: Counter = Counter()
    first_time = None
    last_time = None

    for path in paths:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                raw_lines += 1
                try:
                    message = json.loads(line)
                    validate_topic1_message(message)
                except ValueError as error:
                    if "text must not be empty" in str(error):
                        empty_text += 1
                    else:
                        invalid += 1
                    continue
                if message["message_id"] in seen_ids:
                    duplicate_ids += 1
                    continue
                seen_ids.add(message["message_id"])
                # same rule part 2 uses: identical trimmed text counts as a duplicate
                normalized_text = message["text"].strip()
                if normalized_text in seen_texts:
                    duplicate_texts += 1
                    continue
                seen_texts.add(normalized_text)
                sources[message["source"]] += 1
                first_time = min(first_time or message["event_time"], message["event_time"])
                last_time = max(last_time or message["event_time"], message["event_time"])

    cleaned = len(seen_texts)
    return {
        "files": [str(path) for path in paths],
        "raw_messages": raw_lines,
        "invalid_messages": invalid,
        "empty_text_messages": empty_text,
        "duplicate_message_ids": duplicate_ids,
        "duplicate_texts": duplicate_texts,
        "cleaned_messages": cleaned,
        "by_source": dict(sources),
        "first_event_time": first_time,
        "last_event_time": last_time,
        "meets_1000_requirement": cleaned >= REQUIRED_MESSAGES,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Count raw vs cleaned topic1 messages.")
    parser.add_argument("files", nargs="+", type=Path, help="one or more captured JSONL files")
    parser.add_argument("--output", type=Path, help="optional JSON file to save the counts")
    args = parser.parse_args()
    for path in args.files:
        if not path.exists():
            parser.error(f"file not found: {path}")
    counts = count_messages(args.files)
    text = json.dumps(counts, indent=2)
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
