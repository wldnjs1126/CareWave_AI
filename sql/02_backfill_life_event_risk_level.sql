-- Optional historical backfill, after risk_level column exists.
-- This does not modify non-NULL risk_level values or unknown event types.
-- Check policy with your team before applying to historical data.

SELECT event_type, COUNT(*) AS event_count
FROM life_event
GROUP BY event_type;

-- Uncomment to backfill:
-- UPDATE life_event
-- SET risk_level = CASE
--     WHEN event_type IN ('PRESENCE', 'MOVEMENT', 'BED_STAY', 'SLEEP')
--         THEN '정상'
--     WHEN event_type IN ('ABSENCE', 'NIGHT_EXIT')
--         THEN '관찰'
--     WHEN event_type = 'INACTIVITY' THEN '주의'
--     WHEN event_type = 'FALL' THEN '위험'
--     ELSE risk_level
-- END
-- WHERE risk_level IS NULL
--   AND event_type IN (
--       'PRESENCE', 'MOVEMENT', 'BED_STAY', 'SLEEP',
--       'ABSENCE', 'NIGHT_EXIT', 'INACTIVITY', 'FALL'
--   );
