"""Interactively label rows in a blind text CSV."""

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Optional

from src.common.schemas import AG_NEWS_LABELS, MANUAL_ONLY_LABEL

SAFE_COLUMNS = {"message_id", "text", "my_label"}
LABEL_CHOICES = {
    "1": AG_NEWS_LABELS[0],
    "2": AG_NEWS_LABELS[1],
    "3": AG_NEWS_LABELS[2],
    "4": AG_NEWS_LABELS[3],
    "5": MANUAL_ONLY_LABEL,
}


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("label_csv_path", type=Path)
    return parser.parse_args(argv)


def read_keypress() -> str:
    if not sys.stdin.isatty():
        raise RuntimeError("Interactive labeling requires a terminal")

    if os.name == "nt":
        import msvcrt

        return msvcrt.getwch()

    import termios
    import tty

    file_descriptor = sys.stdin.fileno()
    previous_settings = termios.tcgetattr(file_descriptor)
    try:
        tty.setraw(file_descriptor)
        return sys.stdin.read(1)
    finally:
        termios.tcsetattr(file_descriptor, termios.TCSADRAIN, previous_settings)


def load_label_sheet(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if (
        path.name.casefold() == "predictions.csv"
        or path.resolve().name.casefold() == "predictions.csv"
    ):
        raise ValueError("Pass a blind label sheet, not predictions.csv")

    with path.open(encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)
        fieldnames = reader.fieldnames
        if not fieldnames or any(not name for name in fieldnames):
            raise ValueError("CSV must have a header row")
        if len(fieldnames) != len(set(fieldnames)):
            raise ValueError("CSV header contains duplicate columns")
        if set(fieldnames) - SAFE_COLUMNS:
            raise ValueError("CSV must contain only message_id, text, and my_label columns")
        if not {"text", "my_label"}.issubset(fieldnames):
            raise ValueError("CSV must contain text and my_label columns")

        rows = []
        for row in reader:
            if None in row:
                raise ValueError("CSV row contains more values than the approved header")
            rows.append({name: row.get(name) or "" for name in fieldnames})

    return fieldnames, rows


def save_label_sheet(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(argv: Optional[list[str]] = None) -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = parse_args(argv)
    if not args.label_csv_path.is_file():
        raise FileNotFoundError(f"Label CSV not found: {args.label_csv_path}")

    fieldnames, rows = load_label_sheet(args.label_csv_path)
    total = len(rows)
    for index, row in enumerate(rows, start=1):
        if row["my_label"].strip():
            continue

        print(f"\n[{index}/{total}] {row['text']}")
        while True:
            print(
                "1 World | 2 Sports | 3 Business | 4 Sci/Tech | "
                "5 None of these | q save+quit > ",
                end="",
                flush=True,
            )
            key = read_keypress()
            print(key)

            if key.casefold() == "q":
                save_label_sheet(args.label_csv_path, fieldnames, rows)
                print("Saved label sheet; quitting.")
                return 0

            label = LABEL_CHOICES.get(key)
            if label is None:
                print("Choose 1, 2, 3, 4, 5, or q.")
                continue

            row["my_label"] = label
            save_label_sheet(args.label_csv_path, fieldnames, rows)
            break

    print("No unlabeled rows remain.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
