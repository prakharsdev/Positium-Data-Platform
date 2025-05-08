-- sql/events_per_hour_2024_10_01.sql
CREATE OR REPLACE VIEW events_per_hour_2024_10_01 AS
SELECT 
    DATE_TRUNC('hour', timestamp) AS hour,
    COUNT(*) AS event_count
FROM 
    mobile_events_uncompressed
WHERE 
    DATE(timestamp) = '2024-10-01'
GROUP BY 
    hour
ORDER BY 
    hour;
