
WITH stg_address as (
    SELECT 
        *
    FROM
        {{ source('public', 'address') }}
),

stg_stateprovince as (
    SELECT
        *
    FROM
        {{ source('public', 'stateprovince') }}
),

stg_countryregion as (
    SELECT 
        *
    FROM 
        {{ source('public', 'countryregion') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_address.addressid']) }} as address_key,
    stg_address.addressid,
    stg_address.city as city_name,
    stg_stateprovince.name as state_name,
    stg_countryregion.name as country_name
FROM 
    stg_address
LEFT JOIN 
    stg_stateprovince on stg_address.stateprovinceid = stg_stateprovince.stateprovinceid
LEFT JOIN 
    stg_countryregion on stg_stateprovince.countryregioncode = stg_countryregion.countryregioncode