-- sql/agg_users_per_lau_per_day.sql
CREATE OR REPLACE VIEW agg_users_per_lau_per_day AS
SELECT
    l.lau_id,
    l.lau_name,
    e.date,
    COUNT(DISTINCT e.user_id) AS user_count
FROM mobile_events_compressed e
JOIN cell_plan cp ON e.cell_id::text = cp.cell_id
JOIN lau l
  ON ST_Within(
      ST_Transform(ST_SetSRID(ST_MakePoint(cp.longitude, cp.latitude), 4326), 3035),
      l.geometry
  )
GROUP BY l.lau_id, l.lau_name, e.date;
