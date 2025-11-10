import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

# Ensure the src directory is on sys.path so implicit namespace packages work
CURRENT_FILE = Path(__file__).resolve()
SRC_DIR = CURRENT_FILE.parent
PROJECT_ROOT = SRC_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from extractors.telemetry_reader import read_telemetry_file  # type: ignore
from extractors.error_parser import (
    ErrorEvent,
    parse_events,
    aggregate_events,
)  # type: ignore
from outputs.report_generator import generate_report  # type: ignore

CONFIG_PATH = SRC_DIR / "config" / "settings.example.json"
DEFAULT_DATA_PATH = PROJECT_ROOT / "data" / "sample_logs.json"

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    logging.info("Configuration loaded from %s", path)
    return config

def run_pipeline(config: Dict[str, Any], data_path: Path) -> Path:
    logger = logging.getLogger("pipeline")

    # 1. Ingest telemetry data
    logger.info("Reading telemetry data from %s", data_path)
    raw_events = read_telemetry_file(data_path)
    logger.info("Loaded %d raw telemetry records", len(raw_events))

    if not raw_events:
        logger.warning("No telemetry records found. Exiting.")
        return PROJECT_ROOT / "data" / "error_report.txt"

    # 2. Normalize and enrich events
    logger.info("Parsing and normalizing telemetry events")
    parsed_events: List[ErrorEvent] = parse_events(raw_events, config)
    logger.info("Parsed %d events successfully", len(parsed_events))

    # 3. Aggregate and analyze patterns
    logger.info("Aggregating error statistics")
    summary = aggregate_events(parsed_events)

    # 4. Generate report
    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Generating report in %s", output_dir)
    report_path = generate_report(parsed_events, summary, output_dir)

    # 5. Optionally persist normalized data as JSON for downstream tools
    normalized_path = output_dir / "normalized_events.json"
    with normalized_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(e) for e in parsed_events], f, default=str, indent=2)
    logger.info("Normalized events written to %s", normalized_path)

    # 6. Evaluate alerting thresholds
    alerting_cfg = config.get("alerting", {})
    critical_threshold = int(alerting_cfg.get("critical_error_threshold", 1))
    critical_count = summary["by_severity"].get("CRITICAL", 0)

    if critical_count >= critical_threshold:
        logger.error(
            "Critical alert: %d CRITICAL events detected (threshold=%d)",
            critical_count,
            critical_threshold,
        )
    else:
        logger.info(
            "Critical events below threshold: %d (threshold=%d)",
            critical_count,
            critical_threshold,
        )

    return report_path

def main(argv: List[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    setup_logging()
    logger = logging.getLogger("main")

    try:
        config = load_config(CONFIG_PATH)
    except Exception as exc:
        logger.exception("Failed to load configuration: %s", exc)
        return 1