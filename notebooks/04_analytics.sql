-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Analytics
-- MAGIC Tiga pertanyaan bisnis di atas star schema.

-- COMMAND ----------

USE spotify;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1. Top 5 artis berdasarkan jam dengar, kuartal terakhir
-- MAGIC "Kuartal terakhir" = 3 bulan terakhir dalam data, dihitung dari tanggal stream terbaru.

-- COMMAND ----------

WITH bounds AS (
  SELECT MAX(d.date) AS max_date
  FROM fact_streams f
  JOIN dim_date d ON f.date_key = d.date_key
)
SELECT
  t.artist_name,
  ROUND(SUM(f.ms_played) / 3600000.0, 2) AS total_hours,
  COUNT(*) AS total_plays
FROM fact_streams f
JOIN dim_track t ON f.track_id = t.track_id
JOIN dim_date d  ON f.date_key = d.date_key
CROSS JOIN bounds b
WHERE d.date > add_months(b.max_date, -3)
GROUP BY t.artist_name
ORDER BY total_hours DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2. Rata-rata skip rate (%) per jam
-- MAGIC Versi UTC sesuai soal, lalu versi WIB (UTC+7).

-- COMMAND ----------

SELECT
  hour AS hour_utc,
  COUNT(*) AS total_streams,
  ROUND(100.0 * SUM(CASE WHEN skipped THEN 1 ELSE 0 END) / COUNT(*), 2) AS skip_rate_pct
FROM fact_streams
GROUP BY hour
ORDER BY hour;

-- COMMAND ----------

SELECT
  hour(from_utc_timestamp(stream_ts, 'Asia/Jakarta')) AS hour_wib,
  COUNT(*) AS total_streams,
  ROUND(100.0 * SUM(CASE WHEN skipped THEN 1 ELSE 0 END) / COUNT(*), 2) AS skip_rate_pct
FROM fact_streams
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q3. Top 3 lagu paling overplayed
-- MAGIC Lagu dengan lebih dari 10 putaran dalam satu hari kalender.
-- MAGIC Satu baris per lagu, memakai hari dengan putaran terbanyak.

-- COMMAND ----------

WITH daily AS (
  SELECT
    f.track_id,
    d.date,
    COUNT(*) AS plays_that_day
  FROM fact_streams f
  JOIN dim_date d ON f.date_key = d.date_key
  GROUP BY f.track_id, d.date
  HAVING COUNT(*) > 10
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY plays_that_day DESC) AS rn
  FROM daily
)
SELECT
  t.track_name,
  t.artist_name,
  r.date AS peak_day,
  r.plays_that_day
FROM ranked r
JOIN dim_track t ON r.track_id = t.track_id
WHERE r.rn = 1
ORDER BY r.plays_that_day DESC
LIMIT 3;
