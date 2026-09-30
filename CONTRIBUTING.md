# Contributing

This project supports the club's F1 Telemetry & Race Analytics group. Contributions
from both the data engineering and data science sides are welcome.

## Scope boundaries

- **This repo**: extraction, cleaning, warehousing, orchestration, the MCP
  server, and the dashboard. Changes here should keep the marts stable and
  well-tested, since the data science group depends on them.
- **Not this repo**: modeling notebooks, degradation-curve or pit-window
  algorithms, ML experimentation. Those live in the data science group's own
  space and should treat this repo's marts as a read-only data source.

## Getting started

1. Fork or branch from `main`.
2. Follow the Quickstart in `README.md` to get the stack running locally.
3. Run `dbt test` before opening a PR if you touched anything under
   `transform/dbt/`.

## Making changes

- **New dbt models**: add to `transform/dbt/models/`, include at least one
  test (`unique`, `not_null`, or a custom test) per new mart.
- **New Airflow tasks**: add to `dags/`, keep tasks idempotent — a re-run for
  the same race weekend should not duplicate data.
- **MCP tools**: add to `mcp_server/`, keep all exposed queries read-only.
- **Dashboard changes**: add to `dashboard/`, read only from marts (not
  staging/raw tables).

## Pull requests

- Keep PRs scoped to one layer where possible (ingestion, dbt, MCP,
  dashboard) — easier to review, easier to revert.
- Describe what changed and why in the PR description; link the relevant
  club project discussion if applicable.
- At least one other contributor should review before merging to `main`.

## Reporting issues

Open a GitHub issue with:
- What you expected vs what happened
- Which layer it's in (if known)
- Steps to reproduce, if applicable
