"""
Pipeline orchestrator.

Runs the full Bronze → Silver pipeline in sequence, with timing,
error handling, and a structured summary at the end.

Usage
-----
    python -m pipeline.run              # normal run
    python -m pipeline.run --force      # force re-extraction even if bronze exists
    python -m pipeline.run --bronze-only  # extract only, skip transform
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from pipeline import config
from pipeline.extract import extract
from pipeline.logging_config import get_logger
from pipeline.transform import transform

logger = get_logger(__name__)


def run(force: bool = False, bronze_only: bool = False) -> dict:
    """
    Execute the hospital data pipeline.

    Parameters
    ----------
    force : bool
        Force re-extraction from OpenStreetMap even if a bronze file
        already exists for today's partition.
    bronze_only : bool
        Stop after the bronze (extraction) stage. Useful for debugging
        network connectivity or inspecting raw OSM data before transforming.

    Returns
    -------
    dict
        A summary dict describing the pipeline run outcome.
    """
    start_time = time.monotonic()
    run_at = datetime.now(timezone.utc).isoformat()

    logger.info("=" * 60)
    logger.info("HOSPITAL PIPELINE START")
    logger.info("partition_date=%s  force=%s  bronze_only=%s", config.PARTITION_DATE, force, bronze_only)
    logger.info("=" * 60)

    summary = {
        "run_at": run_at,
        "partition_date": config.PARTITION_DATE,
        "pipeline_version": config.PIPELINE_VERSION,
        "status": "FAILED",
        "bronze_path": None,
        "silver_path": None,
        "elapsed_seconds": None,
        "error": None,
    }

    try:
        # ── Stage 1: Extract (Bronze) ──────────────────────────────────────
        logger.info("── STAGE 1: EXTRACT (BRONZE) ──")
        t0 = time.monotonic()
        bronze_path = extract(force=force)
        summary["bronze_path"] = bronze_path
        logger.info("Extract complete in %.1fs", time.monotonic() - t0)

        if bronze_only:
            logger.info("--bronze-only flag set — stopping after extraction.")
            summary["status"] = "PARTIAL (bronze only)"
            return summary

        # ── Stage 2: Transform (Silver) ────────────────────────────────────
        logger.info("── STAGE 2: TRANSFORM (SILVER) ──")
        t0 = time.monotonic()
        silver_path = transform(bronze_path)
        summary["silver_path"] = silver_path
        logger.info("Transform complete in %.1fs", time.monotonic() - t0)

        summary["status"] = "SUCCESS"

    except Exception as exc:
        logger.error("Pipeline failed: %s: %s", type(exc).__name__, exc, exc_info=True)
        summary["error"] = f"{type(exc).__name__}: {exc}"
        raise

    finally:
        elapsed = time.monotonic() - start_time
        summary["elapsed_seconds"] = round(elapsed, 2)

        logger.info("=" * 60)
        logger.info(
            "PIPELINE %s | %.1fs elapsed | partition=%s",
            summary["status"],
            elapsed,
            config.PARTITION_DATE,
        )
        logger.info("=" * 60)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract UK hospital locations from OpenStreetMap and store using medallion architecture."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-extraction from OpenStreetMap even if today's bronze file already exists.",
    )
    parser.add_argument(
        "--bronze-only",
        action="store_true",
        dest="bronze_only",
        help="Run extraction only; skip the transform stage.",
    )
    args = parser.parse_args()

    try:
        run(force=args.force, bronze_only=args.bronze_only)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
