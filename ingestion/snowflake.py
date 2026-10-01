"""Snowflake raw-layer load for ingestion output.

Layout: database `f1_telemetry`, schema `raw` (from `SNOWFLAKE_SCHEMA`),
tables `laps` and `telemetry`. The `raw_` prefix is dropped because the schema
already carries that meaning. dbt builds `staging.stg_laps` /
`staging.stg_telemetry` on top of these in the transform layer.

Idempotency: loading a session is `DELETE <session key>` then `INSERT`, both
inside one transaction. Re-running for the same session replaces its rows
rather than appending duplicates, and a failed insert rolls back to the
previously loaded data.
"""

from __future__ import annotations

import logging
import math
from contextlib import contextmanager
from typing import Iterator

import pandas as pd
import snowflake.connector as sf

from ingestion.config import SnowflakeConfig

logger = logging.getLogger(__name__)

LAPS_TABLE = "laps"
TELEMETRY_TABLE = "telemetry"

# Key columns that scope an idempotent delete. Must match SessionRef.
SESSION_KEY_COLUMNS = ("season", "event_name", "session_name")

_SESSION_KEY_TYPES = """
    season        NUMBER(4,0)    NOT NULL,
    event_round   NUMBER(4,0),
    event_name    VARCHAR(200)  NOT NULL,
    session_name  VARCHAR(100)  NOT NULL,
    event_date    DATE          NOT NULL
"""

LAPS_DDL = f"""
CREATE TABLE IF NOT EXISTS {{qualified}} (
    {_SESSION_KEY_TYPES},
    driver_number          VARCHAR(10)   NOT NULL,
    driver_code            VARCHAR(10),
    lap_number             NUMBER(4,0),
    stint_number           NUMBER(4,0),
    tyre_life              NUMBER(4,0),
    compound               VARCHAR(20),
    fresh_tyre             BOOLEAN,
    has_lap_time           BOOLEAN,
    lap_time_seconds       FLOAT,
    sector_1_seconds       FLOAT,
    sector_2_seconds       FLOAT,
    sector_3_seconds       FLOAT,
    pit_in_time_seconds    FLOAT,
    pit_out_time_seconds   FLOAT,
    session_time_seconds   FLOAT,
    lap_start_timestamp    TIMESTAMP_NTZ,
    speed_i1_kph           FLOAT,
    speed_i2_kph           FLOAT,
    speed_fl_kph           FLOAT,
    speed_st_kph           FLOAT,
    position_on_track      NUMBER(4,0),
    track_status           VARCHAR(20),
    is_accurate            BOOLEAN,
    is_personal_best       BOOLEAN,
    is_deleted             BOOLEAN,
    deleted_reason         VARCHAR(200),
    team_name_source       VARCHAR(100),
    driver_id              VARCHAR(50),
    full_name              VARCHAR(100),
    team_id                VARCHAR(50),
    team_name              VARCHAR(100),
    country_code           VARCHAR(10),
    loaded_at              TIMESTAMP_NTZ  NOT NULL
)
"""

TELEMETRY_DDL = f"""
CREATE TABLE IF NOT EXISTS {{qualified}} (
    {_SESSION_KEY_TYPES},
    driver_number          VARCHAR(10)   NOT NULL,
    driver_code            VARCHAR(10),
    lap_number             NUMBER(4,0),
    sample_index           NUMBER(9,0),
    sample_timestamp       TIMESTAMP_NTZ,
    sample_offset_seconds  FLOAT,
    session_time_seconds   FLOAT,
    speed_kph              FLOAT,
    throttle_pct           FLOAT,
    brake                  BOOLEAN,
    rpm                    FLOAT,
    gear                   NUMBER(3,0),
    drs                    NUMBER(3,0),
    sample_source          VARCHAR(20),
    is_interpolated        BOOLEAN,
    loaded_at              TIMESTAMP_NTZ  NOT NULL
)
"""


def _to_bindable(series: pd.Series) -> list:
    """Convert one column to Python natives the connector can bind.

    snowflake-connector 4.7.5's pyformat binder accepts Python ``int``,
    ``float``, ``str``, ``bool``, ``None``, ``datetime.datetime`` and
    ``datetime.date``, but rejects ``numpy`` integer scalars (they get
    stringified into the SQL as ``NP.INT64``), ``pandas.Timestamp`` and
    ``pandas.NaT``. Every column is therefore lowered to plain Python values
    with real ``None`` for missing data.
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        # `.array.to_pydatetime()` returns an ndarray of `datetime.datetime`
        # without the pandas 2.x FutureWarning that `Series.dt.to_pydatetime`
        # raises about its changing return type.
        values = list(series.array.to_pydatetime())
    elif pd.api.types.is_bool_dtype(series):
        values = list(series.astype(object).where(series.notna(), None))
    elif pd.api.types.is_integer_dtype(series):
        as_float = series.to_numpy(dtype="float64", na_value=float("nan"))
        values = [None if math.isnan(v) else int(v) for v in as_float]
    elif pd.api.types.is_float_dtype(series):
        as_float = series.to_numpy(dtype="float64", na_value=float("nan"))
        values = [None if math.isnan(v) else float(v) for v in as_float]
    else:
        values = list(series.astype(object).where(series.notna(), None))

    return [
        None if isinstance(v, float) and math.isnan(v) else v for v in values
    ]


class SnowflakeLoader:
    """Thin wrapper around the Snowflake connection for the raw layer."""

    def __init__(self, config: SnowflakeConfig) -> None:
        self.config = config
        self._connection: sf.SnowflakeConnection | None = None

    # -- connection lifecycle ------------------------------------------

    @contextmanager
    def connection(self) -> Iterator[sf.SnowflakeConnection]:
        """Open a connection with autocommit off, close it on exit.

        Autocommit is deliberately off: the delete-then-insert pair per table
        must commit or roll back as one unit for idempotency to hold.
        """
        conn = sf.connect(
            account=self.config.account,
            user=self.config.user,
            private_key_file=self.config.private_key_path,
            role=self.config.role,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=self.config.schema,
            autocommit=False,
        )
        try:
            yield conn
        finally:
            conn.close()

    # -- helpers -------------------------------------------------------

    def qualified(self, table: str) -> str:
        """Fully-qualified quoted table name.

        Snowflake folds unquoted identifiers to upper case, so a quoted
        lower-case name from `.env.local` would not resolve. Normalise to
        upper case before quoting.
        """
        return (
            f'"{self.config.database.upper()}"'
            f'. "{self.config.schema.upper()}"'
            f'. "{table.upper()}"'
        )

    def ensure_tables(self, conn: sf.SnowflakeConnection) -> None:
        cur = conn.cursor()
        try:
            for table, ddl in (
                (LAPS_TABLE, LAPS_DDL),
                (TELEMETRY_TABLE, TELEMETRY_DDL),
            ):
                cur.execute(ddl.format(qualified=self.qualified(table)))
            conn.commit()
            logger.info(
                "Raw tables ready in %s.%s", self.config.database, self.config.schema
            )
        finally:
            cur.close()

    @staticmethod
    def _bindable_rows(df: pd.DataFrame) -> tuple[list[str], list[tuple]]:
        """Return `(columns, rows)` with every value a connector-bindable native.

        The values are deliberately kept as plain Python lists and never put
        back into a DataFrame: assigning a list of `datetime` objects to a
        DataFrame column makes pandas re-coerce it to `datetime64`, and
        iterating that yields `pandas.Timestamp` again -- which the connector
        rejects as an unsupported bind type.
        """
        columns = list(df.columns)
        column_values = [_to_bindable(df[col]) for col in columns]
        return columns, list(zip(*column_values))

    def _delete_session(
        self,
        cur: sf.cursor.SnowflakeCursor,
        table: str,
        key: dict[str, object],
    ) -> int:
        where = " AND ".join(f"{col} = %s" for col in SESSION_KEY_COLUMNS)
        cur.execute(
            f"DELETE FROM {self.qualified(table)} WHERE {where}",
            [key[col] for col in SESSION_KEY_COLUMNS],
        )
        return cur.rowcount

    def _insert(
        self,
        conn: sf.SnowflakeConnection,
        table: str,
        df: pd.DataFrame,
        chunk_size: int = 5000,
    ) -> int:
        """Bulk-insert a frame using a chunked parameterised multi-row INSERT.

        `write_pandas` is deliberately not used: it stages data through
        parquet, and this machine's Application Control policy blocks
        `pyarrow.parquet`, so the connector raises ImportError. A
        parameterised insert needs no local file staging at all.
        """
        if df.empty:
            logger.warning("Refusing to insert 0 rows into %s", table)
            return 0

        ready_columns, rows = self._bindable_rows(df)
        # The DDL declares columns unquoted, so Snowflake folded them to upper
        # case; quoted identifiers are case-sensitive, so match that here.
        collist = ", ".join(f'"{c.upper()}"' for c in ready_columns)
        placeholders = ", ".join(["%s"] * len(ready_columns))
        sql = (
            f"INSERT INTO {self.qualified(table)} ({collist}) "
            f"VALUES ({placeholders})"
        )

        cur = conn.cursor()
        try:
            for start in range(0, len(rows), chunk_size):
                cur.executemany(sql, rows[start : start + chunk_size])
        finally:
            cur.close()
        logger.info("Inserted %d rows into %s", len(rows), table)
        return len(rows)

    # -- public API ----------------------------------------------------

    def load_session(
        self,
        conn: sf.SnowflakeConnection,
        key: dict[str, object],
        laps: pd.DataFrame,
        telemetry: pd.DataFrame,
    ) -> dict[str, int]:
        """Replace this session's rows in both raw tables.

        Returns the row counts written per table.
        """
        loaded_at = pd.Timestamp.utcnow().tz_localize(None)
        cur = conn.cursor()
        written: dict[str, int] = {}
        try:
            deleted_laps = self._delete_session(cur, LAPS_TABLE, key)
            deleted_telemetry = self._delete_session(cur, TELEMETRY_TABLE, key)
            logger.info(
                "Cleared prior rows for %s: laps=%d telemetry=%d",
                key,
                deleted_laps,
                deleted_telemetry,
            )

            written[LAPS_TABLE] = self._insert(
                conn, LAPS_TABLE, laps.assign(loaded_at=loaded_at)
            )
            written[TELEMETRY_TABLE] = self._insert(
                conn, TELEMETRY_TABLE, telemetry.assign(loaded_at=loaded_at)
            )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.error("Load failed; rolled back to previous state", exc_info=True)
            raise
        finally:
            cur.close()
        return written

    def count_rows(
        self,
        conn: sf.SnowflakeConnection,
        key: dict[str, object],
    ) -> dict[str, int]:
        """Row counts per raw table for one session."""
        where = " AND ".join(f"{col} = %s" for col in SESSION_KEY_COLUMNS)
        params = [key[col] for col in SESSION_KEY_COLUMNS]
        cur = conn.cursor()
        try:
            counts = {}
            for table in (LAPS_TABLE, TELEMETRY_TABLE):
                cur.execute(
                    f"SELECT COUNT(*) FROM {self.qualified(table)} WHERE {where}",
                    params,
                )
                counts[table] = cur.fetchone()[0]
            return counts
        finally:
            cur.close()
