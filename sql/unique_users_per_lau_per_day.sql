-- sql/unique_users_per_lau_per_day.sql
CREATE OR REPLACE VIEW unique_users_per_lau_per_day AS
SELECT 
    lau.lau_id,
    lau.lau_name,
    e.date,
    COUNT(DISTINCT e.user_id) AS user_count
FROM mobile_events_uncompressed e
JOIN cell_plan cp ON e.cell_id = cp.cell_id
JOIN lau ON ST_Contains(lau.geometry, ST_Transform(ST_SetSRID(ST_MakePoint(cp.longitude, cp.latitude), 4326), 3035))
GROUP BY lau.lau_id, lau.lau_name, e.date
ORDER BY e.date, lau.lau_id;
