"""Sample prediction rows for blind hand-labeling."""

import argparse
import csv
import random
from pathlib import Path
from typing import Optional

SAMPLE_SIZE = 50
SEED = 4371


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("predictions_csv_path", type=Path)
    parser.add_argument("label_sheet_csv_path", type=Path)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    if args.label_sheet_csv_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing label sheet: {args.label_sheet_csv_path}")
    with args.predictions_csv_path.open(encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)
        required = {"message_id", "text"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError("Predictions CSV must contain message_id and text columns")
        rows = list(reader)

    if len(rows) < SAMPLE_SIZE:
        raise ValueError(f"Need at least {SAMPLE_SIZE} predictions; found {len(rows)}")
    sampled = random.Random(SEED).sample(rows, SAMPLE_SIZE)
    args.label_sheet_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with args.label_sheet_csv_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(["message_id", "text", "my_label"])
        writer.writerows([[row["message_id"], row["text"], ""] for row in sampled])
    print(f"Wrote {SAMPLE_SIZE} blind-label rows to: {args.label_sheet_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
