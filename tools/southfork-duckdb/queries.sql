-- Snapshot freshness and origin.
SELECT * FROM import_metadata;

-- Total attendance and date range (one row per PAX attendance, not per workout).
SELECT count(*) AS posts, count(DISTINCT pax) AS unique_pax,
       min(date) AS first_post, max(date) AS latest_post
FROM posts;

-- Most attended AOs across the available history.
SELECT ao, count(*) AS posts, count(DISTINCT pax) AS unique_pax
FROM posts GROUP BY ao ORDER BY posts DESC LIMIT 20;

-- Monthly attendance for the most recent 12 months present in the snapshot.
SELECT * FROM monthly_attendance
WHERE month >= (SELECT date_trunc('month', max(date)) - INTERVAL '11 months' FROM posts)
ORDER BY month DESC, posts DESC;

-- PAX with the most recorded Qs.
SELECT * FROM pax_summary ORDER BY qs DESC, posts DESC LIMIT 20;

-- QSource counts are separate from regular workout attendance.
SELECT count(*) AS qsource_posts, count(DISTINCT pax) AS unique_pax
FROM qsource_posts;
