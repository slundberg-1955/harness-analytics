-- Migrate extension heuristic columns (v2): drop legacy combined OA + gt90, add CTNF/CTFR split.
-- Startup migration in harness_analytics/schema_migrations.py also applies this shape.

ALTER TABLE application_analytics DROP COLUMN IF EXISTS oa_ext_1mo_count;
ALTER TABLE application_analytics DROP COLUMN IF EXISTS oa_ext_2mo_count;
ALTER TABLE application_analytics DROP COLUMN IF EXISTS oa_ext_3mo_count;
ALTER TABLE application_analytics DROP COLUMN IF EXISTS oa_ext_gt_90d_count;
ALTER TABLE application_analytics DROP COLUMN IF EXISTS ctrs_ext_gt_90d_count;

ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctnf_ext_1mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctnf_ext_2mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctnf_ext_3mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctfr_ext_1mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctfr_ext_2mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctfr_ext_3mo_count INTEGER NOT NULL DEFAULT 0;

-- ctrs_ext_* may already exist; ADD IF NOT EXISTS keeps them.
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctrs_ext_1mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctrs_ext_2mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctrs_ext_3mo_count INTEGER NOT NULL DEFAULT 0;
