-- Run ONLY if SHOW COLUMNS FROM life_event does not show risk_level.
-- This is a manual, one-time migration. It is NOT safe to run twice as-is.
-- Make a database backup / confirm the target database before modifying schema.

SHOW COLUMNS FROM life_event;

-- Uncomment these lines only if risk_level is absent:
-- ALTER TABLE life_event
--   ADD COLUMN risk_level VARCHAR(10) NULL
--   COMMENT '정상, 관찰, 주의, 위험'
--   AFTER confidence;
