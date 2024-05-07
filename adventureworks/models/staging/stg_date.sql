
WITH raw_dates AS (
    SELECT
        date_day,
        day_of_week,
        day_of_week_name,
        day_of_month,
        day_of_year,
        EXTRACT(YEAR FROM date_day) AS year,
        FORMAT_DATETIME('%Y-%m', date_day) AS year_month,
    FROM
        {{ source('public', 'date') }}
)

SELECT
    *
FROM
    raw_dates
