"""Configuration for the ingestion layer.

Environment variables are read from `.env.local` (preferred, machine-specific)
then `.env` (shared defaults). Neither is committed -- see `.gitignore`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Repository root: ingestion/config.py -> ingestion/ -> repo root
REPO_ROOT = Path(__file__).resolve().parent.parent

REQUIRED_ENV_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
)


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or unusable."""


def load_env_files() -> None:
    """Load `.env.local` then `.env` into `os.environ`.

    `.env.local` wins because it holds machine-specific credentials. Real
    environment variables always win over both files.
    """
    load_dotenv(REPO_ROOT / ".env.local", override=True)
    load_dotenv(REPO_ROOT / ".env", override=False)


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    private_key_path: str
    role: str
    warehouse: str
    database: str
    schema: str

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        load_env_files()
        missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
        if missing:
            raise ConfigError(
                "Missing required environment variables: "
                + ", ".join(missing)
                + ". Copy .env.example to .env.local and fill in the values."
            )

        # The key path is relative to the repo root in .env.local; resolve it
        # so the Airflow container and a local venv can both find it.
        key_path = Path(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])
        if not key_path.is_absolute():
            key_path = (REPO_ROOT / key_path).resolve()
        if not key_path.is_file():
            raise ConfigError(f"Snowflake private key not found at {key_path}")

        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key_path=str(key_path),
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
        )


@dataclass(frozen=True)
class SessionRef:
    """Identifies exactly one FastF1 session.

    These four fields are the natural key for both RAW tables, so re-running
    ingestion for the same session replaces its rows rather than duplicating
    them.
    """

    season: int
    event_name: str
    session_name: str
    # FastF1 accepts several spellings per session ("Practice 3", "FP3"); the
    # code is what we store so the column stays small and consistent.
    session_code: str

    def __str__(self) -> str:
        return f"{self.season} {self.event_name} {self.session_name}"


# Maps the FastF1 session code to the display name stored in Snowflake.
SESSION_CODE_TO_NAME = {
    "FP1": "Practice 1",
    "FP2": "Practice 2",
    "FP3": "Practice 3",
    "Q": "Qualifying",
    "Q1": "Qualifying 1",
    "Q2": "Qualifying 2",
    "Q3": "Qualifying 3",
    "Sprint": "Sprint Shootout",
    "Sprint Q": "Sprint Qualifying",
    "R": "Race",
}

SESSION_NAME_TO_CODE = {v: k for k, v in SESSION_CODE_TO_NAME.items()}
