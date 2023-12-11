CREATE OR REPLACE VIEW SEED_DATA.DEV.V_DIM_DATE
AS
-- Leverage ROW_NUMBER to ensure a gap-free sequence.
-- This is a CTE to allow "ROW_NUMBER" to be leveraged in window functions.
WITH "GAPLESS_ROW_NUMBERS" AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY seq4()) - 1 as "ROW_NUMBER" 
  FROM TABLE(GENERATOR(rowcount => 366 * (2100 - 1970)) ) -- rowcount is 366 days x (2100 - 1970) years to cover leap years. A later filter can remove the spillover days
)
SELECT
    DATEADD('day', "ROW_NUMBER", DATE(0)) as "DATE" -- Dimension starts on 1970-01-01 but a different value can be entered if desired
  , EXTRACT(year FROM "DATE") as "YEAR"
  , EXTRACT(month FROM "DATE") as "MONTH"
  , EXTRACT(day FROM "DATE") as "DAY"
  , EXTRACT(dayofweek FROM "DATE") as "DAY_OF_WEEK"
  , EXTRACT(dayofyear FROM "DATE") as "DAY_OF_YEAR"
  , EXTRACT(quarter FROM "DATE") as "QUARTER"
  , MIN("DAY_OF_YEAR") OVER (PARTITION BY "YEAR", "QUARTER") as "QUARTER_START_DAY_OF_YEAR"
  , "DAY_OF_YEAR" - "QUARTER_START_DAY_OF_YEAR" + 1 as "DAY_OF_QUARTER"
  , TO_VARCHAR("DATE", 'MMMM') as "MONTH_NAME"
  , TO_VARCHAR("DATE", 'MON') as "MONTH_NAME_SHORT"
  , CASE "DAY_OF_WEEK"
     WHEN 0 THEN 'Sunday'
     WHEN 1 THEN 'Monday'
     WHEN 2 THEN 'Tuesday'
     WHEN 3 THEN 'Wednesday'
     WHEN 4 THEN 'Thursday'
     WHEN 5 THEN 'Friday'
     WHEN 6 THEN 'Saturday'
    END as "DAY_NAME"
  , TO_VARCHAR("DATE", 'DY') as "DAY_NAME_SHORT"
  , EXTRACT(yearofweekiso FROM "DATE") as "ISO_YEAR"
  , EXTRACT(weekiso FROM "DATE") as "ISO_WEEK"
  , CASE
      WHEN "ISO_WEEK" <= 13 THEN 1
      WHEN "ISO_WEEK" <= 26 THEN 2
      WHEN "ISO_WEEK" <= 39 THEN 3
      ELSE 4
    END as "ISO_QUARTER"
  , EXTRACT(dayofweekiso FROM "DATE") as "ISO_DAY_OF_WEEK"
  , MAX("DAY_OF_YEAR") OVER (PARTITION BY "YEAR") as "DAYS_IN_YEAR"
  , "DAYS_IN_YEAR" - "DAY_OF_YEAR" as "DAYS_REMAINING_IN_YEAR"
FROM "GAPLESS_ROW_NUMBERS"
WHERE "YEAR" < 2100 -- WHERE clause then restricts back to desired timeframe since 366 days per year when generating row numbers is too many
ORDER BY 1 DESC