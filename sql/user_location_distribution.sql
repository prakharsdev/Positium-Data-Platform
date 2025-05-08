-- sql/user_location_distribution.sql
CREATE OR REPLACE VIEW user_location_distribution AS
SELECT unique_locations, COUNT(*) as user_count
FROM (
    SELECT user_id, COUNT(DISTINCT cell_id) as unique_locations
    FROM mobile_events_uncompressed
    GROUP BY user_id
) sub
GROUP BY unique_locations
ORDER BY unique_locations;
