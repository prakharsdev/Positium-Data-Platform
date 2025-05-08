-- sql/agg_unique_users_per_hour.sql
CREATE OR REPLACE VIEW agg_unique_users_per_hour AS
SELECT
    DATE_TRUNC('hour', timestamp) AS hour,
    COUNT(DISTINCT user_id) AS unique_user_count
FROM mobile_events_compressed
GROUP BY hour
ORDER BY hour;
