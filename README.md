# Harness analytics

Ingest Patent Center **Biblio XML** (one file per application) into **PostgreSQL**, classify prosecution events, compute per-application analytics (OAs before first NOA, interviews, RCEs, billing attorney / office), and export **Excel** reports for issued cases in 2024–2025.

## Prerequisites

- Python 3.11+
- PostgreSQL 15+

## Setup

```bash
cd harness-analytics
python3 -m pip install -e ".[dev]"
createdb harness_analytics
python3 -m harness_analytics init --db-url postgresql://localhost/harness_analytics
```

## Office mapping

Office labels are **not** in the XML. Edit [config/office_map.json](config/office_map.json):

- `uspto_customer_number_to_office`: map `CustomerNumber` from the XML to a short office name (for example `"30594": "DC"`).
- `area_code_to_office`: used when customer number is unknown; defaults include `703` and `571` → `DC`.

You can pass a different file with `--office-map /path/to/office_map.json`.

## HDP / Samsung client numbers

`applications.hdp_customer_number` defaults to the USPTO `CustomerNumber` at ingest. If Harness uses separate HDP identifiers, populate `hdp_customer_number` via a one-off SQL update or extend the ingester once the mapping is defined. Reports filter Samsung clients using **either** `hdp_customer_number` or `customer_number`.

## Ingest thousands of XML files

```bash
python3 -m harness_analytics ingest \
  --db-url postgresql://localhost/harness_analytics \
  --folder /path/to/xml_folder \
  --commit-every 50 \
  --recursive \
  --interview-window 90 \
  --error-log ./ingest_errors.jsonl
```

Useful flags:

| Flag | Purpose |
|------|--------|
| `--commit-every N` | Commit after every N **successful** imports (default 50; `0` = single commit at end). |
| `--recursive` | Find `*.xml` in subfolders. |
| `--limit K` | Process only the first K files (smoke tests). |
| `--overwrite` | Replace existing application rows (and children) by application number. |
| `--no-xml-raw` | Do not store full XML in `applications.xml_raw` (saves space). |
| `--skip-analytics` | Ingest only; run `python3 -m harness_analytics analytics ...` later. |

Failed files append one JSON line per error to `--error-log` without rolling back prior successful batches (per-file `SAVEPOINT`).

## Recompute analytics only

```bash
python3 -m harness_analytics analytics \
  --db-url postgresql://localhost/harness_analytics \
  --interview-window 90 \
  --office-map ./config/office_map.json
```

## Excel report

```bash
python3 -m harness_analytics report \
  --db-url postgresql://localhost/harness_analytics \
  --output harness_2024_2025_report.xlsx
```

## One-shot: init + ingest + report

```bash
python3 -m harness_analytics all \
  --db-url postgresql://localhost/harness_analytics \
  --folder /path/to/xml_folder/ \
  --output harness_2024_2025_report.xlsx
```

## Tests

```bash
python3 -m pytest tests/ -v
```

CI runs the same suite on every push to `main` / `master` and on pull requests (see [.github/workflows/ci.yml](.github/workflows/ci.yml)).

## GitHub

Create a new repository from this folder (replace the URL with your account or org):

```bash
cd harness-analytics
git init
git add .
git commit -m "Initial commit: Harness prosecution analytics"
git branch -M main
gh repo create harness-analytics --public --source=. --remote=origin --push
```

If the name `harness-analytics` is taken, pick another name: `gh repo create my-harness-analytics ...`.

## Railway

This repo includes a **Dockerfile** and [railway.toml](railway.toml) so Railway builds a small **FastAPI** service (`harness_analytics/server.py`) that listens on **`PORT`** and exposes **`GET /health`** for deployments. The CLI still handles ingest, analytics, and Excel; run those from your laptop against Railway Postgres, or add a **Railway Cron** / one-off shell with the same image.

### One-time setup

1. **Create a Railway project** (CLI): `cd harness-analytics && railway init -n harness-analytics`  
   Or create a project in the [Railway dashboard](https://railway.app/) and **link** it: `railway link`.

2. **Add PostgreSQL**: in the project, **New** → **Database** → **PostgreSQL**.

3. **Connect the database to the app service**: open your **web service** → **Variables** → **Add reference** → choose the Postgres plugin’s **`DATABASE_URL`** (Railway injects it into the service).

4. **Deploy**: from the repo root, `railway up` (or connect the GitHub repo under **Settings → Service → Source** for automatic deploys on push).

### What Railway runs

- **Container start**: `uvicorn harness_analytics.server:app` on `$PORT` (see `Dockerfile` `CMD`).
- **Pre-deploy** ([railway.toml](railway.toml)): if `DATABASE_URL` is set, runs `python -m harness_analytics init --db-url "$DATABASE_URL"` so tables exist before the new revision goes live.

### CLI against Railway Postgres

With the Postgres plugin selected, Railway shows **`DATABASE_URL`**. Locally:

```bash
export DATABASE_URL='postgresql://...'   # paste from Railway (postgres:// is normalized automatically)
python3 -m harness_analytics ingest --folder /path/to/xml --commit-every 50
python3 -m harness_analytics report --output report.xlsx
```

Omit `--db-url` when `DATABASE_URL` is set.

### Optional checks

- **`GET /health`**: process is up (used for Railway health checks).
- **`GET /health/db`**: tries `SELECT 1` against `DATABASE_URL` (returns 503 if missing or failing).

## Core metric

Substantive examiner actions (**non-final** and **final** OAs) are counted only for `prosecution_events` dated **before** the first **Notice of Allowance** event (`event_type = 'NOA'`, not internal verification). Interview detection uses **FileContentHistory** plus **IFW** document codes/descriptions (for example `INTSUM`, `EXINTSUM`).
