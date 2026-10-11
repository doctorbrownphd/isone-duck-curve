# Data quality

Counts are from the local pipeline run on 2026-10-05. Study period 2019-06-01 to 2026-09-29.

## Sources and volumes

| Source | Raw files | Raw rows | After cleaning |
|---|---|---|---|
| ISO-NE hourly system load (web services API, one JSON per day) | 2,679 | 64,296 hourly records | 64,294 rows in `curated/hourly_load` |
| NOAA Local Climatological Data v2 (one CSV per station per year) | 48 | 659,920 rows of all kinds | 406,766 hourly rows in `curated/hourly_weather` |
| ISO-NE installed solar by year (hand-entered) | 1 | 8 year-end values | joined by year |

The daily table has 2,678 rows, one per date, with 492 duck days.

## Station list

One major airport per New England state. Daily weather is the unweighted mean across stations, so stations are not weighted by where the electricity demand is. That is a limitation.

| State | Station | NOAA id |
|---|---|---|
| MA | Boston Logan | USW00014739 |
| CT | Hartford Bradley | USW00014740 |
| RI | Providence T.F. Green | USW00014765 |
| ME | Portland Jetport | USW00014764 |
| NH | Concord Municipal | USW00014745 |
| VT | Burlington | USW00014742 |

Worcester was considered and rejected: its LCD files for 2019 to 2022 hold no hourly data.

## Hourly load

- **Duplicates:** 2 exact duplicate hourly records in the API responses (2020-09-27 hour ending 15, 2020-10-30 hour ending 21). Rule: keep one. Removed: 2.
- **Missing native load values:** 0.
- **Missing hours:** two days have 23 rows outside the daylight saving dates (2025-02-28 missing hour ending 9, 2025-06-25 missing hour ending 18). Rule: a day is dropped only if more than 2 hourly rows are missing. Days dropped: 0. On both days the lowest hour is far from the missing one.
- **Daylight saving:** the spring day has 23 rows because the 2:00 hour does not exist. The fall day has 25 rows because the 1:00 hour happens twice. Both rows of the repeated hour are kept, with the same hour ending (2) and different UTC timestamps. In the study period there are 7 spring days and 7 fall days. Both transitions are overnight, so neither can make or unmake a duck day by itself, but the repeated hour does compete for the daily minimum like any other hour.
- **Time convention:** ISO-NE gives the start of each hour in local prevailing time with a UTC offset. Hour ending is the local start hour plus one. Each row also stores the UTC timestamp.
- **Ties for the daily minimum:** none. The rule, if one occurred, is to keep the earlier hour.

## Hourly weather

- **Row kinds:** an LCD file mixes routine hourly reports (FM-15: 406,766 rows), special reports (FM-16: 116,423), synoptic reports (FM-12: 118,683), daily summaries (SOD: 16,962), monthly summaries (SOM: 558) and a few others (528). Rule: keep FM-15 only. Special reports are issued when conditions change, so keeping them would over-count bad weather hours.
- **Time convention:** LCD times are local standard time all year. Rule: add 5 hours for UTC, then convert to America/New_York so weather and load share a clock. Checked against a second NOAA hourly product that is in UTC: a 5 hour shift matches exactly in January and July, a 4 hour shift does not.
- **Temperature:** LCD v2 gives degrees Celsius. Converted to Fahrenheit. No values carried suffix flags in these files (0 flagged), though the code handles them.
- **Sky cover rule:** each report lists cloud layers such as `FEW:02 15.24 BKN:07 30.48`. The two digits are the layer's coverage in oktas: CLR 0, FEW 2, SCT 4, BKN 7, OVC 8. The hour's value is the highest layer. Obscured sky (VV:09) counts as 8. Airport sensors only detect cloud up to about 12,000 ft, so high thin cloud reads as clear.
- **Missing values in the hourly rows:**

| Station | Hourly rows | Temperature missing | Sky cover missing |
|---|---|---|---|
| Boston | 67,687 | 17 | 874 |
| Burlington | 67,841 | 23 | 1,283 |
| Concord | 67,766 | 601 | 3,345 |
| Hartford Bradley | 67,790 | 8 | 780 |
| Portland | 67,878 | 13 | 1,723 |
| Providence | 67,804 | 55 | 1,591 |

## Daily weather

- **Daily high:** the highest hourly temperature of the local day at each station. A station needs at least 18 hourly readings that day.
- **Daytime sky cover:** the mean of hourly sky cover from 9:00 to 16:00 local time at each station. A station needs at least 5 of the 7 reports.
- **Gap rule:** the regional value is the mean of the stations that have a value that day. Missing stations are filled by the others.
- **How often that happened:** temperature used all 6 stations on 2,633 days, 5 on 36 days and 4 on 1 day. Sky cover used all 6 on 2,494 days, 5 on 136, 4 on 26, and 3 or fewer on 14. Most of the thin days are in 2025 (71 days below 6 stations) and 2026 (50).
- **Days with no weather at all:** 8. They stay in the daily table with empty weather columns and are left out of the model.

| Date | Reason | Duck day |
|---|---|---|
| 2023-06-01 | every station has only 6 hourly reports that day | no |
| 2025-08-30, 2025-08-31, 2025-09-01 | no hourly reports at any station in NOAA's files | yes, yes, no |
| 2025-09-02 | too few reports | no |
| 2026-03-26 | too few daytime reports | yes |
| 2026-09-28, 2026-09-29 | NOAA had not yet published these days when the files were downloaded on 2026-10-05 | no, no |

## Model rows

2,670 of 2,678 days have every predictor: 2,040 in training (251 duck days), 361 in the 2025 test year (132), 269 in the 2026 holdout (106). Three duck days are among the 8 days left out.

## Solar

One value per calendar year: the installed PV capacity ISO-NE reported for the previous December 31. The figures cover all distributed PV reported by utilities, which is wider than strictly behind-the-meter. The year-end 2025 value comes from a forecast document's "Thru 2025" column rather than a survey table.
