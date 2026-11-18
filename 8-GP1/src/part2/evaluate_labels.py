"""Compare completed hand labels with the saved model predictions."""

import argparse
import csv
from pathlib import Path
from typing import Optional

from src.common.schemas import AG_NEWS_LABELS, MANUAL_ONLY_LABEL

VALID_LABELS = set(AG_NEWS_LABELS.values()) | {MANUAL_ONLY_LABEL}


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("predictions_csv_path", type=Path)
    parser.add_argument("completed_label_sheet_path", type=Path)
    parser.add_argument("output_csv_path", type=Path)
    return parser.parse_args(argv)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as input_file:
        return list(csv.DictReader(input_file))


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    if args.output_csv_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing evaluation: {args.output_csv_path}")

    predictions = read_csv(args.predictions_csv_path)
    label_rows = read_csv(args.completed_label_sheet_path)
    prediction_by_id = {row["message_id"]: row for row in predictions}
    if len(prediction_by_id) != len(predictions):
        raise ValueError("Predictions CSV contains duplicate message_id values")

    merged: list[dict[str, str]] = []
    for label_row in label_rows:
        message_id = label_row.get("message_id", "")
        manual_label = label_row.get("my_label", "").strip()
        if manual_label not in VALID_LABELS:
            raise ValueError(
                f"Invalid or empty my_label for message_id {message_id!r}; "
                f"choose one of {sorted(VALID_LABELS)}"
            )
        prediction = prediction_by_id.get(message_id)
        if prediction is None:
            raise ValueError(f"No prediction found for labeled message_id {message_id!r}")
        is_in_scope = manual_label != MANUAL_ONLY_LABEL
        is_correct = str(manual_label == prediction["predicted_class"]) if is_in_scope else ""
        merged.append(
            {
                "message_id": message_id,
                "text": label_row.get("text", prediction["text"]),
                "my_label": manual_label,
                "predicted_class": prediction["predicted_class"],
                "confidence": prediction.get("confidence", ""),
                "is_in_scope": str(is_in_scope),
                "is_correct": is_correct,
            }
        )

    args.output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "message_id",
        "text",
        "my_label",
        "predicted_class",
        "confidence",
        "is_in_scope",
        "is_correct",
    ]
    with args.output_csv_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged)

    none_count = sum(row["my_label"] == MANUAL_ONLY_LABEL for row in merged)
    in_scope_rows = [row for row in merged if row["my_label"] != MANUAL_ONLY_LABEL]
    correct_count = sum(row["is_correct"] == "True" for row in in_scope_rows)
    accuracy = correct_count / len(in_scope_rows) if in_scope_rows else None
    print(f"None of these: {none_count}")
    print(f"In-scope labels: {len(in_scope_rows)}")
    print(f"In-scope accuracy: {accuracy:.6f}" if accuracy is not None else "In-scope accuracy: N/A")
    print("Wrong in-scope labels:")
    print("message_id | my_label | predicted_class | text")
    wrong_rows = [row for row in in_scope_rows if row["is_correct"] == "False"]
    for row in wrong_rows:
        print(
            f"{row['message_id']} | {row['my_label']} | {row['predicted_class']} | {row['text']}"
        )
    print(f"Wrote hand-label comparison to: {args.output_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
