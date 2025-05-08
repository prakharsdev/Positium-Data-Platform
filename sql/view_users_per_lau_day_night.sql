-- sql/view_users_per_lau_day_night.sql
CREATE OR REPLACE VIEW view_users_per_lau_day_night AS
WITH lau_geoms AS (
    SELECT lau_id, lau_name, geometry
    FROM lau
),
cells AS (
    SELECT
        cell_id,
        ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 3035) AS geom_3035
    FROM cell_plan
),
cell_lau AS (
    SELECT
        c.cell_id,
        l.lau_id,
        l.lau_name
    FROM cells c
    JOIN lau_geoms l
      ON ST_Within(c.geom_3035, l.geometry)
),
events_filtered AS (
    SELECT user_id, cell_id, timestamp
    FROM mobile_events_uncompressed
    WHERE timestamp::date = DATE '2024-10-01'
),
joined_events AS (
    SELECT
        e.user_id,
        e.timestamp,
        cl.lau_id
    FROM events_filtered e
    JOIN cell_lau cl ON e.cell_id = cl.cell_id
    WHERE cl.lau_id IS NOT NULL
),
classified_events AS (
    SELECT
        user_id,
        lau_id,
        CASE
            WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 7 AND 19 THEN 'day'
            ELSE 'night'
        END AS period
    FROM joined_events
),
agg AS (
    SELECT
        lau_id,
        period,
        COUNT(DISTINCT user_id) AS user_count
    FROM classified_events
    GROUP BY lau_id, period
)
SELECT
    l.lau_id,
    l.lau_name,
    COALESCE(d.user_count, 0) AS day_users,
    COALESCE(n.user_count, 0) AS night_users
FROM lau_geoms l
LEFT JOIN agg d ON l.lau_id = d.lau_id AND d.period = 'day'
LEFT JOIN agg n ON l.lau_id = n.lau_id AND n.period = 'night';

