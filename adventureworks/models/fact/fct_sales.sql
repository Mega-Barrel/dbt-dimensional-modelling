
WITH stg_salesorderheader AS (
    SELECT
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status AS order_status,
        CAST(orderdate AS DATE) AS orderdate
    FROM
        {{ source('public', 'salesorderheader') }}
),

stg_salesorderdetail AS (
    SELECT
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        unitprice * orderqty AS revenue
    FROM
        {{ source('public', 'salesorderdetail') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_salesorderdetail.salesorderid', 'salesorderdetailid']) }} as sales_key,
    {{ dbt_utils.generate_surrogate_key(['productid']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['creditcardid']) }} as creditcard_key,
    {{ dbt_utils.generate_surrogate_key(['shiptoaddressid']) }} as ship_address_key,
    {{ dbt_utils.generate_surrogate_key(['order_status']) }} as order_status_key,
    {{ dbt_utils.generate_surrogate_key(['orderdate']) }} as order_date_key,
    stg_salesorderdetail.salesorderid,
    stg_salesorderdetail.salesorderdetailid,
    stg_salesorderdetail.unitprice,
    stg_salesorderdetail.orderqty,
    stg_salesorderdetail.revenue
FROM
    stg_salesorderdetail
INNER JOIN
    stg_salesorderheader
    ON stg_salesorderdetail.salesorderid = stg_salesorderheader.salesorderid
