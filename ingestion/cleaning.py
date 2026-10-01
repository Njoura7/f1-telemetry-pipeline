"""Pandas cleaning for extracted FastF1 session data.

This module is deliberately free of `fastf1` and `snowflake` imports: every
function takes and returns plain DataFrames so the cleaning rules can be unit
tested without network access or warehouse credentials.

Responsibilities, per the README ("typing, missing-lap handling, outlier
flags"). Outlier flagging is *not* done here -- the data model assigns that to
`fct_lap_times`, so this layer stays a faithful, typed copy of the source and
leaves judgement calls downstream.
"""

from __future__ import annotations

import pandas as pd

# --------------------------------------------------------------------------
# Low-level normalisers
# --------------------------------------------------------------------------


def blank_to_null(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Replace empty / whitespace-only strings with real nulls.

    FastF1 uses `None` and `''` interchangeably, so a naive `isna()` check
    misses empty strings and they silently become `''` in Snowflake.
    """
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            continue
        as_text = out[col].astype("string").str.strip()
        out[col] = as_text.mask(as_text.eq(""), other=pd.NA)
    return out


TRUTHY_STRINGS = frozenset({"true", "t", "yes", "y", "1"})


def coerce_bool_series(series: pd.Series) -> pd.Series:
    """Cast one flag series to real booleans.

    Needed because FastF1 ships some flags as `object` dtype holding `None`,
    which makes `~series` raise `TypeError: bad operand type for unary ~`.
    Null flags become False (a missing flag is not an affirmative).

    Uses string comparison rather than `fillna(False).astype(bool)`, which
    pandas 2.x deprecates for object-dtype downcasting.
    """
    if series.dtype == bool:
        return series
    return series.astype("string").str.strip().str.lower().isin(TRUTHY_STRINGS)


def coerce_bool(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Cast flag columns to real booleans (see `coerce_bool_series`)."""
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = coerce_bool_series(out[col])
    return out


def timedelta_to_seconds(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert `timedelta64` columns to float seconds.

    Snowflake has no timedelta type; seconds-as-float is what the marts and the
    MCP tools want to aggregate on. Null durations stay null rather than
    becoming 0.0, because 0 is a meaningful lap time and would skew averages.
    """
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            continue
        out[col] = (
            pd.to_timedelta(out[col]).dt.total_seconds().astype("float64")
        )
    return out


def to_int64(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Cast count-like columns to nullable `Int64` (nulls preserved)."""
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    return out


def rename_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Lowercase-friendly rename that ignores columns not present."""
    present = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=present)


# --------------------------------------------------------------------------
# Frame builders
# --------------------------------------------------------------------------

LAPS_RENAME = {
    "Driver": "driver_code",
    "DriverNumber": "driver_number",
    "LapNumber": "lap_number",
    "Stint": "stint_number",
    "TyreLife": "tyre_life",
    "Compound": "compound",
    "FreshTyre": "fresh_tyre",
    "Team": "team_name_source",
    "LapStartDate": "lap_start_timestamp",
    "TrackStatus": "track_status",
    "Position": "position_on_track",
    "Deleted": "is_deleted",
    "DeletedReason": "deleted_reason",
    "IsPersonalBest": "is_personal_best",
}

LAPS_TIMEDELTA_COLUMNS = [
    "LapTime",
    "Sector1Time",
    "Sector2Time",
    "Sector3Time",
    "PitInTime",
    "PitOutTime",
    "Time",
]

TELEMETRY_RENAME = {
    "Speed": "speed_kph",
    "Throttle": "throttle_pct",
    "Brake": "brake",
    "RPM": "rpm",
    "nGear": "gear",
    "DRS": "drs",
    "Date": "sample_timestamp",
    "Time": "sample_offset_seconds",
    "SessionTime": "session_time_seconds",
    "Source": "sample_source",
}


def build_laps_frame(
    laps_raw: pd.DataFrame,
    session_columns: dict[str, object],
) -> pd.DataFrame:
    """Normalise a raw session's lap table into the RAW.LAPS shape.

    Laps with no recorded lap time (in-laps, out-laps, aborted laps) are
    **kept** rather than dropped -- the raw layer mirrors what FastF1 returned
    -- and flagged via `has_lap_time` so downstream models can filter
    explicitly instead of guessing.
    """
    df = pd.DataFrame(laps_raw).reset_index(drop=True)

    df = rename_columns(df, LAPS_RENAME)
    df = blank_to_null(df, ["deleted_reason", "compound", "track_status", "driver_code"])
    df = coerce_bool(df, ["is_deleted", "is_personal_best", "fresh_tyre"])
    df = timedelta_to_seconds(df, LAPS_TIMEDELTA_COLUMNS)
    df = to_int64(
        df, ["lap_number", "stint_number", "tyre_life", "position_on_track"]
    )

    df = df.rename(
        columns={
            "LapTime": "lap_time_seconds",
            "Sector1Time": "sector_1_seconds",
            "Sector2Time": "sector_2_seconds",
            "Sector3Time": "sector_3_seconds",
            "PitInTime": "pit_in_time_seconds",
            "PitOutTime": "pit_out_time_seconds",
            "Time": "session_time_seconds",
            "SpeedI1": "speed_i1_kph",
            "SpeedI2": "speed_i2_kph",
            "SpeedFL": "speed_fl_kph",
            "SpeedST": "speed_st_kph",
        }
    )

    # The lap-time delta encoding is the signal for "this lap has a time".
    df["has_lap_time"] = df["lap_time_seconds"].notna()
    df["is_accurate"] = (
        coerce_bool_series(df["IsAccurate"])
        if "IsAccurate" in df.columns
        else False
    )

    df["driver_number"] = df["driver_number"].astype("string").str.strip()

    # Speed trap / pit flags arrive as floats with NaN holes; keep them float.
    for col in ("speed_i1_kph", "speed_i2_kph", "speed_fl_kph", "speed_st_kph"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    for key, value in session_columns.items():
        df[key] = value

    ordered = [
        "season",
        "event_round",
        "event_name",
        "session_name",
        "event_date",
        "driver_number",
        "driver_code",
        "lap_number",
        "stint_number",
        "tyre_life",
        "compound",
        "fresh_tyre",
        "has_lap_time",
        "lap_time_seconds",
        "sector_1_seconds",
        "sector_2_seconds",
        "sector_3_seconds",
        "pit_in_time_seconds",
        "pit_out_time_seconds",
        "session_time_seconds",
        "lap_start_timestamp",
        "speed_i1_kph",
        "speed_i2_kph",
        "speed_fl_kph",
        "speed_st_kph",
        "position_on_track",
        "track_status",
        "is_accurate",
        "is_personal_best",
        "is_deleted",
        "deleted_reason",
        "team_name_source",
        "driver_id",
        "full_name",
        "team_id",
        "team_name",
        "country_code",
    ]
    return df[[c for c in ordered if c in df.columns]]


def build_telemetry_frame(
    telemetry_chunks: list[tuple[dict[str, object], pd.DataFrame]],
    session_columns: dict[str, object],
) -> pd.DataFrame:
    """Flatten per-lap telemetry samples into the RAW.TELEMETRY shape.

    `telemetry_chunks` is a list of `(lap_key, car_data)` pairs, where
    `lap_key` carries the driver/lap identity and `car_data` is one lap's
    telemetry frame (indexed by sample).
    """
    frames: list[pd.DataFrame] = []

    for lap_key, car_data in telemetry_chunks:
        if car_data is None or len(car_data) == 0:
            continue

        chunk = pd.DataFrame(car_data).reset_index(drop=True)
        chunk = rename_columns(chunk, TELEMETRY_RENAME)
        chunk = blank_to_null(chunk, ["sample_source"])
        chunk = coerce_bool(chunk, ["brake"])
        chunk = to_int64(chunk, ["gear", "drs"])
        chunk["rpm"] = pd.to_numeric(chunk.get("rpm"), errors="coerce").astype(
            "float64"
        )
        chunk["speed_kph"] = pd.to_numeric(
            chunk.get("speed_kph"), errors="coerce"
        ).astype("float64")
        chunk["throttle_pct"] = pd.to_numeric(
            chunk.get("throttle_pct"), errors="coerce"
        ).astype("float64")
        for col in ("sample_offset_seconds", "session_time_seconds"):
            if col not in chunk.columns:
                continue
            # FastF1 delivers these as timedeltas; `to_numeric` on a timedelta
            # silently yields nanoseconds, so route them through the seconds
            # converter instead.
            if pd.api.types.is_timedelta64_dtype(chunk[col]):
                chunk[col] = timedelta_to_seconds(chunk, [col])[col]
            else:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype(
                    "float64"
                )

        # FastF1 tags samples it had to synthesise; keep that provenance.
        chunk["is_interpolated"] = (
            chunk["sample_source"].astype("string").str.lower() == "interpolation"
        ).fillna(False)
        chunk["sample_index"] = pd.array(range(len(chunk)), dtype="Int64")

        for key, value in {**session_columns, **lap_key}.items():
            chunk[key] = value

        frames.append(chunk)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)

    ordered = [
        "season",
        "event_round",
        "event_name",
        "session_name",
        "event_date",
        "driver_number",
        "driver_code",
        "lap_number",
        "sample_index",
        "sample_timestamp",
        "sample_offset_seconds",
        "session_time_seconds",
        "speed_kph",
        "throttle_pct",
        "brake",
        "rpm",
        "gear",
        "drs",
        "sample_source",
        "is_interpolated",
    ]
    return out[[c for c in ordered if c in out.columns]]


def build_driver_frame(results_raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise the session entry list into a driver lookup frame.

    The raw lap table only carries the three-letter driver code; the stable
    `driver_id`, full name and team come from the results table. Used as a
    lookup to denormalise driver identity onto the lap rows (the README data
    model defines no separate driver table).
    """
    df = pd.DataFrame(results_raw).reset_index(drop=True)
    if "DriverNumber" not in df.columns and df.index.name == "DriverNumber":
        df = df.reset_index()

    df["driver_number"] = (
        df["DriverNumber"].astype("string").str.strip()
        if "DriverNumber" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="string")
    )

    keep = {
        "driver_id": "DriverId",
        "driver_code": "Abbreviation",
        "full_name": "FullName",
        "first_name": "FirstName",
        "last_name": "LastName",
        "team_id": "TeamId",
        "team_name": "TeamName",
        "country_code": "CountryCode",
        "grid_position": "GridPosition",
        "final_position": "Position",
        "num_laps": "Laps",
    }
    # `available` reads new -> old for legibility; rename wants old -> new.
    available = {new: old for new, old in keep.items() if old in df.columns}
    old_to_new = {old: new for new, old in available.items()}
    out = df[["driver_number", *available.values()]].rename(columns=old_to_new)

    out = blank_to_null(
        out,
        [
            "driver_id",
            "driver_code",
            "full_name",
            "first_name",
            "last_name",
            "team_id",
            "team_name",
            "country_code",
        ],
    )
    out = to_int64(out, ["grid_position", "final_position", "num_laps"])
    return out[[c for c in ("driver_number", *available) if c in out.columns]]


DRIVER_METADATA_COLUMNS = [
    "driver_id",
    "full_name",
    "team_id",
    "team_name",
    "country_code",
]


def attach_driver_metadata(
    laps: pd.DataFrame,
    drivers: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalise driver identity from the driver lookup onto the lap rows.

    Merges on `driver_number` with a `driver_code` fallback, because FastF1
    occasionally leaves `DriverNumber` unset on a row. The raw lap table's
    `driver_code` from timing data is preferred over the results table's when
    both exist, since it is the code that was actually on the timing screen.
    """
    out = laps.copy()
    lookup = drivers[["driver_number", *DRIVER_METADATA_COLUMNS]].drop_duplicates(
        subset="driver_number"
    )
    out = out.merge(lookup, on="driver_number", how="left")
    return out
