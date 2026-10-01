# Ingestion

Extracts one FastF1 session, cleans it with pandas, and loads it into the
Snowflake raw schema. This is step 1 of the pipeline: `FastF1 -> pandas ->
Snowflake raw/staging`.

## Run it

From the repo root, with the venv activated (`.venv\Scripts\activate`):

```powershell
# defaults are the MVP session: 2024 Bahrain Grand Prix Practice 3
python -m ingestion.ingest
```

Explicitly:

```powershell
python -m ingestion.ingest --season 2024 --event "Bahrain Grand Prix" --session FP3
```

Useful flags:

| Flag | Effect |
|---|---|
| `--no-telemetry` | Laps only. Skips the slow per-lap telemetry fetch. |
| `--telemetry-lap-limit N` | Telemetry for only the first `N` on-track laps (chronological). For quick iteration. |
| `--log-level DEBUG` | Verbose FastF1 logging. |

The session can be given as a code (`FP1`, `FP2`, `FP3`, `Q`, `Q1`, `Q2`,
`Q3`, `Sprint`, `R`) or a display name (`Practice 3`).

## What lands where

Database `f1_telemetry`, schema `raw` (from `SNOWFLAKE_SCHEMA`), tables
`laps` and `telemetry`. The `raw_` prefix is dropped because the schema
already carries that meaning — dbt builds `staging.stg_laps` and
`staging.stg_telemetry` on top of these.

Both tables carry the session key (`season`, `event_round`, `event_name`,
`session_name`, `event_date`) on every row, so a session is addressable
without a join. `event_date` is FastF1's event date (the race date), not the
day the session ran — for Bahrain 2024 the event date is 2024-03-02 while FP3
ran on 2024-03-01. Use `lap_start_timestamp` / `sample_timestamp` for real
session timing.

Driver identity (`driver_id`, `full_name`, `team_id`, `team_name`,
`country_code`) is denormalised onto the lap rows from the session entry list;
the README data model defines no separate driver table.

## Cleaning rules

All in `cleaning.py`, which imports neither `fastf1` nor Snowflake so it can
be unit tested in isolation (see `tests/`).

- **Flag coercion.** FastF1 3.8 ships some flags as `object` dtype holding
  `None`, which makes `~df[col]` raise `TypeError: bad operand type for
  unary ~`. Flags are coerced to real booleans; a missing flag is `False`.
- **Empty strings to null.** `None` and `''` are used interchangeably by
  FastF1, so blanks become real nulls instead of empty strings in Snowflake.
- **Durations to seconds.** `timedelta64` columns become float seconds
  (Snowflake has no timedelta type). Null durations stay null rather than
  becoming `0.0`, since `0` is a meaningful lap time and would skew averages.
- **Missing-lap handling.** In-laps, out-laps and aborted laps have no lap
  time. They are **kept** — the raw layer mirrors what FastF1 returned — and
  flagged via `has_lap_time` so downstream models filter explicitly instead of
  guessing.
- **Telemetry times.** `Time` / `SessionTime` are timedeltas and are routed
  through the seconds converter, not `to_numeric`, which would silently
  return nanoseconds.

Outlier flagging is deliberately **not** done here: the README data model
assigns that to `fct_lap_times`, so this layer stays a faithful, typed copy of
the source.

## Idempotency

Loading a session is `DELETE <session key>` then `INSERT`, both in one
transaction. Re-running for the same session replaces its rows instead of
appending duplicates, and a failed insert rolls back to the previous data. This
is what lets the step-3 DAG be re-triggered safely.

`SESSION_KEY_COLUMNS = (season, event_name, session_name)` in `snowflake.py`
defines the key and must stay in sync with `SESSION_KEY_TYPES` in the DDL.

## Notes

- FastF1 reads the env var `FASTF1_CACHE`, not `FASTF1_CACHE_DIR`. Extraction
  sets both and also calls `fastf1.Cache.enable_cache()` explicitly. The cache
  defaults to `.fastf1_cache/` in the repo root (gitignored); a warm cache is
  what keeps re-runs fast.
- `write_pandas` is not used for loading. It stages through parquet, and
  `pyarrow.parquet` is blocked by this machine's Application Control policy.
  `snowflake.py` uses a chunked parameterised multi-row `INSERT` instead, with
  values lowered to the Python natives the connector can bind (it rejects
  `numpy` integer scalars, `pandas.Timestamp` and `pandas.NaT`).
- Loading ~99k telemetry rows takes roughly 3 minutes via that insert path.
  A staged bulk upload would be faster; see v2 in the top-level README.
