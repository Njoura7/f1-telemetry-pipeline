"""Ingestion CLI: extract one FastF1 session, clean it, load it to Snowflake.

Usage (from the repo root, venv activated):

    python -m ingestion.ingest --season 2024 --event "Bahrain Grand Prix" --session FP3

or rely on the defaults baked in for the MVP session:

    python -m ingestion.ingest

Exit code is non-zero on any failure, so Airflow can gate `dbt run` on it.
"""

from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

from ingestion import cleaning
from ingestion.config import (
    SESSION_CODE_TO_NAME,
    ConfigError,
    SessionRef,
    SnowflakeConfig,
)
from ingestion.extract import extract_session
from ingestion.snowflake import SnowflakeLoader

logger = logging.getLogger("ingestion")

# The MVP session. Step 3's DAG calls this with the same arguments, and the
# dashboard defaults to the same session.
DEFAULT_SEASON = 2024
DEFAULT_EVENT = "Bahrain Grand Prix"
DEFAULT_SESSION = "FP3"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.ingest",
        description="Extract, clean and load one FastF1 session to Snowflake raw.",
    )
    parser.add_argument("--season", type=int, default=DEFAULT_SEASON)
    parser.add_argument("--event", default=DEFAULT_EVENT)
    parser.add_argument(
        "--session",
        default=DEFAULT_SESSION,
        help="Session code (FP1, FP2, FP3, Q, R) or display name (Practice 3).",
    )
    parser.add_argument(
        "--no-telemetry",
        action="store_true",
        help="Skip telemetry extraction (faster; laps table only).",
    )
    parser.add_argument(
        "--telemetry-lap-limit",
        type=int,
        default=None,
        help="Cap the number of laps telemetry is pulled for. Default: all "
        "on-track timed laps.",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="FastF1 cache directory. Defaults to .fastf1_cache in the repo root.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args(argv)


def resolve_session_code(session: str) -> str:
    """Accept either a FastF1 code ('FP3') or a display name ('Practice 3')."""
    if session in SESSION_CODE_TO_NAME:
        return session
    for code, name in SESSION_CODE_TO_NAME.items():
        if name.lower() == session.lower():
            return code
    raise SystemExit(
        f"Unknown session {session!r}. Use one of: "
        f"{', '.join(SESSION_CODE_TO_NAME)} (or the full display name)."
    )


def build_frames(
    extracted,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Turn an `ExtractedSession` into the two warehouse-ready frames."""
    ref: SessionRef = extracted.ref
    session_columns: dict[str, object] = {
        "season": ref.season,
        "event_round": extracted.event_round,
        "event_name": ref.event_name,
        "session_name": SESSION_CODE_TO_NAME[ref.session_code],
        "event_date": pd.Timestamp(extracted.event_date).date(),
    }

    drivers = cleaning.build_driver_frame(extracted.drivers)
    laps = cleaning.build_laps_frame(extracted.laps, session_columns)
    laps = cleaning.attach_driver_metadata(laps, drivers)
    telemetry = cleaning.build_telemetry_frame(
        extracted.telemetry_chunks, session_columns
    )
    return laps, telemetry


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    session_code = resolve_session_code(args.session)
    ref = SessionRef(
        season=args.season,
        event_name=args.event,
        session_name=SESSION_CODE_TO_NAME[session_code],
        session_code=session_code,
    )

    try:
        config = SnowflakeConfig.from_env()
    except ConfigError as exc:
        logger.error("%s", exc)
        return 2

    logger.info("Target %s.%s", config.database, config.schema)

    extracted = extract_session(
        ref,
        include_telemetry=not args.no_telemetry,
        telemetry_lap_limit=args.telemetry_lap_limit,
        cache_dir=args.cache_dir,
    )
    logger.info(
        "Extracted %d laps, %d telemetry chunks (%d laps without telemetry)",
        len(extracted.laps),
        len(extracted.telemetry_chunks),
        extracted.laps_without_telemetry,
    )

    laps, telemetry = build_frames(extracted)
    logger.info(
        "Cleaned frames: laps=%d rows telemetry=%d rows",
        len(laps),
        len(telemetry),
    )

    loader = SnowflakeLoader(config)
    key = {
        "season": ref.season,
        "event_name": ref.event_name,
        "session_name": SESSION_CODE_TO_NAME[ref.session_code],
    }

    with loader.connection() as conn:
        loader.ensure_tables(conn)
        written = loader.load_session(conn, key, laps, telemetry)
        counts = loader.count_rows(conn, key)

    logger.info("Written this run: %s", written)
    logger.info("Row counts in warehouse: %s", counts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
