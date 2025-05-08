-- sql/agg_cell_count_per_user_per_day.sql
CREATE OR REPLACE VIEW agg_cell_count_per_user_per_day AS
SELECT
    user_id,
    date,
    COUNT(DISTINCT cell_id) AS unique_cell_count
FROM mobile_events_compressed
GROUP BY user_id, date;
