# Harness analytics

Ingest Patent Center **Biblio XML** (one file per application) into **PostgreSQL**, classify prosecution events, compute per-application analytics (OAs before first NOA, interviews, RCEs, office), and export **Excel** reports for issued cases in 2024–2025.

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

Optional GitHub Actions CI is not committed by default because some `gh` OAuth tokens cannot push workflow files without the `workflow` scope. To enable it, run `gh auth refresh -s workflow`, restore `.github/workflows/ci.yml` from the template below, commit, and push.

```yaml
# .github/workflows/ci.yml
name: CI
on:
  push: { branches: [main, master] }
  pull_request: { branches: [main, master] }
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install -e ".[dev]"
      - run: python -m pytest tests/ -v
```

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

**Ingest** still runs from your laptop and needs the Postgres plugin’s **`DATABASE_PUBLIC_URL`** (the internal `postgres.railway.internal` URL only works inside Railway’s network).

**Analytics** (`scripts/run_railway_analytics.py`) defaults to **`railway ssh`** into the app service so `python -m harness_analytics analytics` uses the container’s **`DATABASE_URL`** (internal). Use **`--local`** on that script if you want the old behavior (local Python + public URL). **`--foreground`** / **`-f`** streams logs over SSH instead of detaching with **`/usr/bin/nohup`** and **`/app/analytics_run.log`**.

With the [Railway CLI](https://docs.railway.com/develop/cli) linked to this repo (`railway link`), you can run the helpers (no copy-paste of credentials):

```bash
cd harness-analytics
# Bulk load (skips analytics during load; run analytics after — much faster for large folders)
PYTHONUNBUFFERED=1 python3 scripts/run_railway_ingest.py
python3 scripts/run_railway_analytics.py          # optional: --foreground, or --local for laptop+public URL
python3 scripts/run_railway_report.py   # writes harness_railway_report.xlsx in repo root
```

Or paste manually:

```bash
export DATABASE_URL='postgresql://...'   # use DATABASE_PUBLIC_URL from Postgres (postgres:// is normalized automatically)
python3 -m harness_analytics ingest --folder /path/to/xml --commit-every 50
python3 -m harness_analytics report --output report.xlsx
```

Omit `--db-url` when `DATABASE_URL` is set.

### Web portal (download + per-matter browser)

The same FastAPI service exposes a **password-protected portal** under **`/portal/`**. Nothing under `/portal` (except the sign-in page) is available until you authenticate.

1. Set **`PORTAL_PASSWORD`** on the app service to a strong secret.
2. Set **`SECRET_KEY`** to a long random string (used to sign session cookies). If you omit it, the app falls back to `PORTAL_PASSWORD` for signing (works, but rotating the password will log everyone out).
3. Optional: **`PORTAL_USER`** — sign-in username (default **`viewer`**).
4. Optional: **`INTERVIEW_WINDOW_DAYS`** — max days from the **last** qualifying IFW interview before the first IFW **NOA** document for the `interview_led_to_noa` flag (default **90**; matches the CLI `analytics` / ingest `--interview-window`). Used by **Recompute analytics** on matter pages.

**Sign in:** open `https://<your-railway-host>/portal/login` (or `/portal/` — browsers asking for HTML are redirected to the login page). Use the HTML form, or use **HTTP Basic** with the same username and password (for scripts and `curl`).

After sign-in, the browser keeps a **signed session cookie** until you click **Sign out** or the cookie expires. **`GET /portal/report.xlsx`** and matter pages require that session (or Basic) on every request.

What you get:

- **`GET /portal/report.xlsx`** — same multi-sheet Excel workbook as the CLI `report` command (link on the home page after login). The main **All Harness IP** sheet is limited to **issue years 2024 and 2025** (issued, status **150**), matching the original reporting scope.
- **`GET /portal/report-all-applications.xlsx`** (alias **`GET /portal/report-all-years.xlsx`**) — same workbook shape, but **every application** in the database (**any** `application_status_code`), with **`LEFT JOIN application_analytics`** so analytics columns are present when computed and null otherwise. Includes **`application_status_code`** and **`application_status_text`** on each row. Summary statistics use one column per distinct issue year present in the data (applications with no `issue_date` / `issue_year` are counted only in sheet totals, not in year columns).
- **`GET /portal/matter/<application_number>`** — HTML summary for one matter: application fields, analytics row, prosecution events and file-wrapper tables (first 500 rows each if larger), a one-row **Excel (All Harness IP) column** preview when an analytics row exists, a **Recompute analytics (this application only)** control, and a link to raw XML when `xml_raw` was stored at ingest.
- **`POST /portal/matter/<application_number>/recompute-analytics`** — recomputes `application_analytics` for that matter only, then redirects back to the matter page (same logic as the CLI `analytics` command, scoped to one application).
- **`POST /portal/recompute-all-analytics/start`** — starts a **background job** that recomputes analytics for **every** application (JSON response with `job_id` and `hint`). Only **one** such job runs at a time per server process; if a job is already pending or running, the response reuses it (`hint: already_active`).
- **`GET /portal/recompute-all-analytics/status/<job_id>`** — JSON progress: `status` (`pending` \| `running` \| `completed` \| `failed`), `total`, `done`, and `error` when failed. The portal home page runs this on an interval after **Recompute all**.
- **`GET /portal/matter/<application_number>/xml`** — raw Biblio XML (`inline` display; large payloads).

`/health` stays **unauthenticated** for Railway probes. If `PORTAL_PASSWORD` is unset, `/portal` routes (except `/portal/login`, which explains the situation) return **503**. Treat this portal as **sensitive**: it can expose client data and full XML.

### Optional checks

- **`GET /health`**: process is up (used for Railway health checks).
- **`GET /health/db`**: tries `SELECT 1` against `DATABASE_URL` (returns 503 if missing or failing).

## Core metric

Substantive **office actions** in analytics are counted from **IFW** only: document code **CTNF** (non-final) and **CTFR** (final), each row dated by mail room date **before** the first IFW document with code **NOA** (allowance). **Interview** signals use IFW only, with document codes **EXIN**, **INTV.SUM.EX**, and **INTV.SUM.APP** (unique mail-room dates). **NOA within 90 days of interview** (`interview_led_to_noa`) is true when the span from the **most recent** qualifying interview date strictly before that first **NOA** to the **NOA** mail date is at most **`INTERVIEW_WINDOW_DAYS`** (default 90). **Days last interview → NOA** stores that span in days. **`ifw_a_ne_count`** is the number of file-wrapper rows whose **`document_code`** is exactly **`A.NE`** after trim and ASCII case fold (periods and all other characters must match the stored code exactly—no aliases or punctuation normalization). **`ifw_ctrs_count`** counts IFW rows with document code **`CTRS`** (restriction requirement) the same way. **`continuity_child_of_prior_us`** on **`applications`** is **true** when the Biblio **`<Continuity>/<ParentContinuityList>`** lists this application as **`ChildApplicationNumber`** with at least one **`ParentApplicationNumber`** that is **not** a **PCT/** parent (prior US-style parent for Harness reporting). It is set at ingest and refreshed when **`analytics`** runs if **`xml_raw`** is stored.

**Extension-of-time heuristics (estimated, not legal USPTO extension counts):** For each **CTNF** and each **CTFR** **before the first IFW NOA**, the code assumes a **3 calendar month** response window from that OA’s IFW mail date and finds the first prosecution history event classified as **`RESPONSE_NONFINAL`**, **`RESPONSE_FINAL`**, or **`RCE`** with `transaction_date` strictly after that OA and strictly before the earlier of the **next** OA mail date (if any) and the first NOA mail date. If that response is after the deadline, lateness in whole days is bucketed into **`ctnf_ext_*`** or **`ctfr_ext_*`** columns: 1–30 days, 31–60, or 61–90 days late only; **lateness beyond 90 days is not stored**. For each IFW **CTRS** before the first NOA, the assumed window is **2 calendar months**, using the same response-event set and the earlier of the **next** CTRS mail date or first NOA as the upper bound; late buckets are **`ctrs_ext_1mo_count`**, **`ctrs_ext_2mo_count`**, **`ctrs_ext_3mo_count`** with the same 90-day cap. Implementation: [`harness_analytics/extension_metrics.py`](harness_analytics/extension_metrics.py).

**Existing databases:** the web app **startup migration** adds missing columns: `application_analytics.ifw_a_ne_count`, `application_analytics.ifw_ctrs_count`, `applications.continuity_child_of_prior_us`, and the nine extension columns (`ctnf_ext_*`, `ctfr_ext_*`, `ctrs_ext_*`). It **drops** legacy extension column names when migrating from older builds, and **drops** legacy `application_analytics.billing_attorney_reg` and `billing_attorney_name` if present. The CLI **`init`** path alone does not alter existing tables. Optional manual DDL: [`scripts/add_schema_continuity_ctrs.sql`](scripts/add_schema_continuity_ctrs.sql), [`scripts/add_extension_columns.sql`](scripts/add_extension_columns.sql). Run **`analytics`** or portal **Recompute analytics** to populate IFW counts, extension heuristics, and refresh continuity from XML when present.
