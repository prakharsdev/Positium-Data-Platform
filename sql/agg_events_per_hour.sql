-- sql/agg_events_per_hour.sql
CREATE OR REPLACE VIEW agg_events_per_hour AS
SELECT
    DATE_TRUNC('hour', timestamp) AS hour,
    COUNT(*) AS event_count
FROM mobile_events_compressed
GROUP BY hour
ORDER BY hour;
