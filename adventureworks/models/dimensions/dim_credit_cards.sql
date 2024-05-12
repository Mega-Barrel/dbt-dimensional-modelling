
with stg_salesorderheader as (
    SELECT 
        distinct creditcardid
    FROM
        {{ ref('salesorderheader') }}
    WHERE
        creditcardid IS NOT NULL
),

stg_creditcard as (
    SELECT 
        *
    FROM
        {{ ref('creditcard') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_salesorderheader.creditcardid']) }} as creditcard_key,
    stg_salesorderheader.creditcardid,
    stg_creditcard.cardtype
FROM 
    stg_salesorderheader
LEFT JOIN
    stg_creditcard on stg_salesorderheader.creditcardid = stg_creditcard.creditcardid
