"""FastF1 extraction. The only module in the pipeline that imports `fastf1`.

Keeping the network boundary here means `cleaning.py` stays unit-testable and
the DAG can swap extraction without touching the load logic.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ingestion.cleaning import coerce_bool_series
from ingestion.config import REPO_ROOT, SessionRef

logger = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = REPO_ROOT / ".fastf1_cache"


@dataclass
class ExtractedSession:
    """Raw, un-cleaned payloads for exactly one session."""

    ref: SessionRef
    event_date: pd.Timestamp
    event_round: int | None
    laps: pd.DataFrame
    drivers: pd.DataFrame
    telemetry_chunks: list[tuple[dict[str, object], pd.DataFrame]]
    laps_without_telemetry: int


def configure_cache(cache_dir: str | os.PathLike[str] | None = None) -> str:
    """Point FastF1's on-disk cache at a predictable location.

    Uses the explicit ``fastf1.Cache.enable_cache`` API rather than an env
    var: FastF1 reads ``FASTF1_CACHE`` (no ``_DIR`` suffix), and setting it
    too keeps child processes consistent. Both are set so the same cache is
    used whether extraction runs from the venv or the Airflow container.

    The DAG re-runs for the same session on every trigger, so a warm cache is
    what keeps repeat runs fast and avoids hammering the F1 API.
    """
    path = Path(cache_dir) if cache_dir else DEFAULT_CACHE_DIR
    path.mkdir(parents=True, exist_ok=True)
    resolved = str(path)
    os.environ["FASTF1_CACHE"] = resolved
    os.environ["FASTF1_CACHE_DIR"] = resolved
    return resolved


def _lap_key(row: pd.Series) -> dict[str, object]:
    return {
        "driver_number": str(row["driver_number"]),
        "driver_code": row["driver_code"],
        "lap_number": row["lap_number"],
    }


def extract_session(
    ref: SessionRef,
    *,
    include_telemetry: bool = True,
    telemetry_lap_limit: int | None = None,
    cache_dir: str | os.PathLike[str] | None = None,
) -> ExtractedSession:
    """Download one session's laps, entry list and per-lap telemetry.

    `telemetry_lap_limit` caps how many laps get telemetry, which keeps the
    first-run load quick; the cap is applied deterministically (chronological
    order) so a re-run extracts the same subset and stays idempotent.
    """
    import fastf1

    cache_path = configure_cache(cache_dir)
    fastf1.Cache.enable_cache(cache_path)

    logger.info(
        "Extracting %s (telemetry=%s, cache=%s)", ref, include_telemetry, cache_path
    )
    session = fastf1.get_session(ref.season, ref.event_name, ref.session_name)
    session.load(
        laps=True,
        telemetry=include_telemetry,
        weather=False,
        messages=False,
    )

    # `session.laps` is a fastf1.core.Laps (a DataFrame subclass) whose rows
    # carry `.get_car_data()`. It must stay a Laps for the whole telemetry
    # loop, so the plain-DataFrame copy handed to the cleaning layer is made
    # only after extraction finishes.
    laps_live = session.laps
    laps_live["Deleted"] = coerce_bool_series(laps_live["Deleted"])

    telemetry_chunks: list[tuple[dict[str, object], pd.DataFrame]] = []
    laps_without_telemetry = 0

    if include_telemetry:
        candidates = telemetry_candidate_laps(laps_live)
        if telemetry_lap_limit is not None:
            candidates = candidates.head(telemetry_lap_limit)

        logger.info(
            "Extracting telemetry for %d of %d laps",
            len(candidates),
            len(laps_live),
        )

        for position in range(len(candidates)):
            lap_row = candidates.iloc[position]
            if position and position % 25 == 0:
                logger.info("  telemetry %d/%d", position, len(candidates))
            try:
                car_data = lap_row.get_car_data()
            except Exception:  # noqa: BLE001 - one bad lap must not kill the run
                logger.warning(
                    "Telemetry unavailable for %s lap %s",
                    lap_row.get("Driver"),
                    lap_row.get("LapNumber"),
                )
                car_data = None

            if car_data is None or len(car_data) == 0:
                laps_without_telemetry += 1
                continue

            lap_key = {
                "driver_number": str(lap_row["DriverNumber"]).strip(),
                "driver_code": lap_row["Driver"],
                "lap_number": lap_row["LapNumber"],
            }
            telemetry_chunks.append((lap_key, car_data))

    return ExtractedSession(
        ref=ref,
        event_date=pd.Timestamp(session.event["EventDate"]),
        event_round=session.event.get("RoundNumber"),
        laps=pd.DataFrame(laps_live).reset_index(drop=True),
        drivers=pd.DataFrame(session.results).reset_index(drop=True),
        telemetry_chunks=telemetry_chunks,
        laps_without_telemetry=laps_without_telemetry,
    )


def telemetry_candidate_laps(laps_live):
    """Laps worth pulling telemetry for: on-track, timed, not deleted.

    Returns a `fastf1.core.Laps` (boolean masking and `sort_values` both
    preserve the subclass), which is required for the `.get_car_data()` calls
    that follow. Filtering here rather than loading telemetry for every row
    skips the in/out laps, which have no on-track samples anyway.
    """
    mask = (
        ~laps_live["Deleted"]
        & pd.to_datetime(laps_live["LapStartDate"], errors="coerce").notna()
        & pd.to_timedelta(laps_live["LapTime"], errors="coerce").notna()
    )
    frame = laps_live.loc[mask]
    return frame.sort_values(
        ["LapStartDate", "DriverNumber"], kind="stable"
    )
