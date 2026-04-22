# Prosecution Timeline — Operations Runbook

Companion to `PROSECUTION_TIMELINE_DESIGN.md`. Tracks the M10 dogfooding
checklist for the timeline feature.

## Full-corpus recompute

```bash
# Recompute every application in the global tenant. Use after a rule change
# or after migrating to a new schema version. Idempotent; safe to re-run.
python -m harness_analytics timeline-recompute --db-url "$DATABASE_URL"

# Single tenant:
python -m harness_analytics timeline-recompute --tenant-id acme --db-url "$DATABASE_URL"
```

The same operation is exposed as the Arq task `timeline_recompute_all` (used
by the Rules admin save flow) and `timeline_recompute_application` (used by
the analytics post-hook on a per-application basis).

## Attorney spot-check workflow

1. Open `/portal/actions` (Upcoming Actions) filtered to the next 30 days.
2. Click into each deadline; verify the date against the docket.
3. Use the `verify` action (in the deadline drawer, surfaces as a button or
   POST `/portal/api/timeline/deadlines/{id}/actions` with
   `{"action": "verify"}`) to stamp it.
4. Subscribed ICS calendars will start showing `STATUS:CONFIRMED` instead of
   `TENTATIVE` on the next refresh.

## Closing top-N unmapped IFW codes

```bash
# Print the top 20 IFW codes seen during materialization that have no rule.
python -m harness_analytics unmapped-codes --top 20 --db-url "$DATABASE_URL"
```

For each code, decide:

* **Add a rule** — go to `/portal/admin/rules`, click "+ New rule" (or use
  the API directly). Pick the closest `kind` and fill in the SSP / max
  months. Save triggers a tenant-wide recompute and the unmapped count
  starts dropping immediately.
* **Alias an existing rule** — if the code is just a synonym for an
  existing rule (e.g. `CTFR.WDR` → `CTFR`), edit the rule and add it to
  the comma-separated `aliases` field.
* **Ignore** — if the code is informational only (filing receipts,
  acknowledgement letters), it's safe to leave unmapped; the materializer
  records the count so you can re-evaluate later.

## Lifting the feature flag

There's no runtime feature flag for the timeline UI. The matter page
(`/portal/matter/{n}`) renders a "Prosecution Timeline" card unconditionally,
the inbox at `/portal/actions` is reachable from the top nav, and the
portfolio's `nextDeadlineDate` column is enabled by default. Hiding the
feature, if ever needed, is a CSS-only change in `templates/base.html`
(remove the nav links) plus a removal of the `#prosecution-timeline` block in
`templates/matter.html`.

## ICS feed

* Per-user URL: `GET /portal/api/me/ics-token` returns
  `https://<host>/portal/ics/<user_id>.ics?token=<random>`.
* Rotate (revokes any existing subscription):
  `POST /portal/api/me/ics-token/rotate`.
* Token storage: `users.ics_token` (added in migration `0005`).

## Schema

* `0005_verified_deadlines` — adds `verified_deadlines` and `users.ics_token`.
* Idempotent fallback DDL lives in `harness_analytics/schema_migrations.py`
  for environments that haven't applied Alembic.
