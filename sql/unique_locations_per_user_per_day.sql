-- sql/unique_locations_per_user_per_day.sql
CREATE OR REPLACE VIEW unique_locations_per_user_per_day AS
SELECT 
    user_id,
    date,
    COUNT(DISTINCT cell_id) AS unique_locations
FROM mobile_events_uncompressed
GROUP BY user_id, date
ORDER BY user_id, date;
