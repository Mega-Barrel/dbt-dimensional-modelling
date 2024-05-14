
WITH stg_date AS (
    SELECT
        *
    FROM
        {{ source('public', 'date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_date.date_day']) }} as date_key,
    *
FROM
    stg_date
