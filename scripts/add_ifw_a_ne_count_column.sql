-- Run once on existing PostgreSQL databases (new installs get the column from ORM create_all).
ALTER TABLE application_analytics
  ADD COLUMN IF NOT EXISTS ifw_a_ne_count INTEGER NOT NULL DEFAULT 0;
