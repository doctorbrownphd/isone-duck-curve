-- Athena checks on the curated tables .
-- Run each statement on its own in the Athena console, database duck_curve.
-- The crawler registers the year partition as a string, so year is compared to '2019', not 2019.

-- 1. Row counts per year, all three tables
SELECT 'hourly_load' AS table_name, year, COUNT(*) AS row_count FROM hourly_load GROUP BY year
UNION ALL
SELECT 'hourly_weather', year, COUNT(*) FROM hourly_weather GROUP BY year
UNION ALL
SELECT 'daily_model', year, COUNT(*) FROM daily_model GROUP BY year
ORDER BY table_name, year;

-- 2. Null counts per column in the daily table
SELECT year,
       COUNT(*) AS days,
       SUM(CASE WHEN daily_high_temp_f IS NULL THEN 1 ELSE 0 END) AS null_daily_high_temp_f,
       SUM(CASE WHEN daytime_sky_cover_oktas IS NULL THEN 1 ELSE 0 END) AS null_daytime_sky_cover_oktas,
       SUM(CASE WHEN btm_solar_mw IS NULL THEN 1 ELSE 0 END) AS null_btm_solar_mw,
       SUM(CASE WHEN min_load_mw IS NULL THEN 1 ELSE 0 END) AS null_min_load_mw,
       SUM(CASE WHEN is_duck_day IS NULL THEN 1 ELSE 0 END) AS null_is_duck_day
FROM daily_model
GROUP BY year
ORDER BY year;

-- 3. Duck days per year. Must match Step 0:
--    2019: 0, 2020: 11, 2021: 18, 2022: 44, 2023: 72, 2024: 106, 2025: 134, 2026: 107
SELECT year, COUNT(*) AS days, SUM(is_duck_day) AS duck_days
FROM daily_model
GROUP BY year
ORDER BY year;

-- 4. Every full year has 365 or 366 days. 2019 starts June 1 (214 days) and
--    2026 ends September 29 (272 days).
SELECT year, COUNT(*) AS days,
       CASE WHEN COUNT(*) IN (365, 366) THEN 'full year'
            WHEN year = '2019' AND COUNT(*) = 214 THEN 'starts Jun 1, as expected'
            WHEN year = '2026' AND COUNT(*) = 272 THEN 'ends Sep 29, as expected'
            ELSE 'CHECK' END AS status
FROM daily_model
GROUP BY year
ORDER BY year;

-- 5. Hourly load: days that do not have 24 rows (daylight saving days and two
--    days with one hour missing at the source)
SELECT "date", COUNT(*) AS hours
FROM hourly_load
GROUP BY "date"
HAVING COUNT(*) <> 24
ORDER BY "date";
