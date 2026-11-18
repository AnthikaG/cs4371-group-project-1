"""Capture Kibana top-10 entity charts and matching Elasticsearch summaries."""

import argparse
import csv
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

LOGGER = logging.getLogger("kibana-interval-capture")


def parse_intervals(value: str) -> list[int]:
    try:
        intervals = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as error:
        raise argparse.ArgumentTypeError("intervals must be comma-separated positive integers") from error
    if not intervals or any(interval <= 0 for interval in intervals):
        raise argparse.ArgumentTypeError("intervals must be comma-separated positive integers")
    return intervals


def parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("--start-time must be an ISO-8601 datetime") from error
    return parsed.astimezone() if parsed.tzinfo is None else parsed


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dashboard-url",
        default=os.getenv("KIBANA_DASHBOARD_URL"),
        help="Kibana dashboard URL (or KIBANA_DASHBOARD_URL)",
    )
    parser.add_argument(
        "--elasticsearch-url",
        default=os.getenv("ELASTICSEARCH_URL"),
        help="Elasticsearch base URL (or ELASTICSEARCH_URL)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=os.getenv("KIBANA_INTERVAL_OUTPUT_DIR"),
        help="output directory (or KIBANA_INTERVAL_OUTPUT_DIR)",
    )
    parser.add_argument("--intervals", type=parse_intervals, default=parse_intervals("15,30,45,60"))
    parser.add_argument("--start-time", type=parse_datetime)
    parser.add_argument("--test", action="store_true", help="capture one screenshot immediately")
    args = parser.parse_args(argv)
    missing = [
        name
        for name, value in (
            ("--dashboard-url or KIBANA_DASHBOARD_URL", args.dashboard_url),
            ("--elasticsearch-url or ELASTICSEARCH_URL", args.elasticsearch_url),
            ("--output-dir or KIBANA_INTERVAL_OUTPUT_DIR", args.output_dir),
        )
        if not value
    ]
    if missing:
        parser.error("missing required configuration: " + ", ".join(missing))
    args.output_dir = Path(args.output_dir)
    return args


def request_json(url: str, payload: Optional[dict] = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"} if data is not None else {},
        method="GET" if data is None else "POST",
    )
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def check_services(dashboard_url: str, elasticsearch_url: str) -> None:
    try:
        with urlopen(dashboard_url, timeout=15) as response:
            if response.status >= 400:
                raise RuntimeError(f"Kibana returned HTTP {response.status}")
    except (OSError, URLError) as error:
        raise RuntimeError(f"Kibana is unreachable at {dashboard_url}: {error}") from error

    try:
        request_json(f"{elasticsearch_url.rstrip('/')}/_cluster/health")
    except (OSError, URLError, ValueError) as error:
        raise RuntimeError(f"Elasticsearch is unreachable at {elasticsearch_url}: {error}") from error


def query_top_entities(elasticsearch_url: str) -> list[dict[str, object]]:
    search_body = {
        "size": 0,
        "aggs": {
            "entities": {
                "terms": {
                    "field": "entity.keyword",
                    "size": 10,
                    "order": {"max_count": "desc"},
                },
                "aggs": {
                    "max_count": {"max": {"field": "count"}},
                    "best_record": {
                        "top_hits": {
                            "size": 1,
                            "sort": [{"count": {"order": "desc"}}],
                            "_source": {"includes": ["entity", "entity_type", "count"]},
                        }
                    },
                },
            }
        },
    }
    try:
        result = request_json(
            f"{elasticsearch_url.rstrip('/')}/named-entities-latest/_search",
            search_body,
        )
    except (OSError, URLError, ValueError) as error:
        raise RuntimeError(f"Elasticsearch top-entity query failed: {error}") from error

    rows = []
    for bucket in result.get("aggregations", {}).get("entities", {}).get("buckets", []):
        hits = bucket.get("best_record", {}).get("hits", {}).get("hits", [])
        source = hits[0].get("_source", {}) if hits else {}
        max_count = bucket.get("max_count", {}).get("value")
        rows.append(
            {
                "entity": bucket.get("key", ""),
                "type": source.get("entity_type", ""),
                "count": "" if max_count is None else int(max_count),
            }
        )
    return rows


def save_entity_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=["entity", "type", "count"])
        writer.writeheader()
        writer.writerows(rows)


def capture(
    page,
    args: argparse.Namespace,
    interval_name: str,
    elapsed_minutes: float,
) -> None:
    image_path = args.output_dir / f"top10_{interval_name}min.png"
    csv_path = args.output_dir / f"top10_{interval_name}min.csv"
    if image_path.exists() or csv_path.exists():
        raise FileExistsError(f"Refusing to overwrite interval output for {interval_name} minutes")

    response = page.goto(args.dashboard_url, wait_until="domcontentloaded", timeout=60_000)
    if response is not None and response.status >= 400:
        raise RuntimeError(f"Kibana dashboard returned HTTP {response.status}")
    page.locator(
        "[data-test-subj='xyVisChart'], canvas.echChart, .echChart"
    ).first.wait_for(state="visible", timeout=120_000)

    rows = query_top_entities(args.elasticsearch_url)
    captured_at = datetime.now().astimezone()
    banner_text = (
        f"Captured {captured_at.strftime('%Y-%m-%d %H:%M:%S %Z')} "
        f"| elapsed {elapsed_minutes:.1f} minutes"
    )
    page.evaluate(
        """(text) => {
            const previous = document.getElementById("interval-capture-banner");
            if (previous) previous.remove();
            const banner = document.createElement("div");
            banner.id = "interval-capture-banner";
            banner.textContent = text;
            Object.assign(banner.style, {
                position: "fixed",
                top: "12px",
                right: "12px",
                zIndex: "2147483647",
                padding: "10px 14px",
                color: "#ffffff",
                background: "#142b4a",
                border: "2px solid #ffffff",
                borderRadius: "4px",
                font: "bold 16px sans-serif",
                boxShadow: "0 2px 8px rgba(0, 0, 0, 0.4)"
            });
            document.body.appendChild(banner);
        }""",
        banner_text,
    )
    page.screenshot(path=str(image_path), full_page=True)
    save_entity_csv(csv_path, rows)
    LOGGER.info(
        "captured interval=%s elapsed_minutes=%.1f screenshot=%s csv=%s top_entities=%d",
        interval_name,
        elapsed_minutes,
        image_path,
        csv_path,
        len(rows),
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(args.output_dir / "run_log.txt", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    started_at = args.start_time or datetime.now().astimezone()
    LOGGER.info("run started t0=%s test=%s", started_at.isoformat(), args.test)

    try:
        check_services(args.dashboard_url, args.elasticsearch_url)
    except RuntimeError as error:
        LOGGER.error("%s", error)
        return 1

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as error:
        LOGGER.error("Playwright is missing; install it with: pip install playwright")
        raise SystemExit(1) from error

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            if args.test:
                capture(page, args, "test", max(0.0, (datetime.now().astimezone() - started_at).total_seconds() / 60))
            else:
                for interval in args.intervals:
                    target = started_at.timestamp() + interval * 60
                    LOGGER.info("scheduled interval=%d target_epoch=%.3f", interval, target)
                    while time.time() < target:
                        time.sleep(min(target - time.time(), 30))
                    elapsed = (datetime.now().astimezone() - started_at).total_seconds() / 60
                    capture(page, args, str(interval), elapsed)
            browser.close()
    except KeyboardInterrupt:
        LOGGER.info("capture interrupted by user")
        return 130
    except Exception:
        LOGGER.exception("interval capture failed")
        return 1

    LOGGER.info("run completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
