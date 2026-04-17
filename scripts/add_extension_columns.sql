-- Optional manual DDL (startup migration also adds these if missing).
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS oa_ext_1mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS oa_ext_2mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS oa_ext_3mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS oa_ext_gt_90d_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctrs_ext_1mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctrs_ext_2mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctrs_ext_3mo_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE application_analytics ADD COLUMN IF NOT EXISTS ctrs_ext_gt_90d_count INTEGER NOT NULL DEFAULT 0;
