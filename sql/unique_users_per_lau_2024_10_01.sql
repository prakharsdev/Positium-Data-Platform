-- sql/unique_users_per_lau_2024_10_01.sql
CREATE OR REPLACE VIEW unique_users_per_lau_2024_10_01 AS
SELECT 
    l.lau_id,
    l.lau_name,
    COUNT(DISTINCT e.user_id) AS user_count
FROM 
    mobile_events_compressed e
JOIN 
    cell_plan c ON e.cell_id = c.cell_id
JOIN 
    lau l ON ST_Contains(l.geometry, ST_Transform(ST_SetSRID(ST_MakePoint(c.longitude, c.latitude), 4326), 3035))
WHERE 
    e.date = '2024-10-01'
GROUP BY 
    l.lau_id, l.lau_name
ORDER BY 
    user_count DESC;
