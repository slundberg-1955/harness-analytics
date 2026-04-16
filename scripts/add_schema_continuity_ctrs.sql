-- Run once on existing PostgreSQL if not using app startup migrations.
ALTER TABLE application_analytics
  ADD COLUMN IF NOT EXISTS ifw_ctrs_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS continuity_child_of_prior_us BOOLEAN NOT NULL DEFAULT false;
