with source as (
    select * from {{ source('raw', 'conversions') }}
),

cleaned as (
    SELECT 
        conversion_id, 
        click_id, 
        customer_id, 
        campaign_id, 
        cast(conversion_date as date) as conversion_date, 
        round(revenue_usd, 2) revenue_usd, 
        lower(conversion_type) as conversion_type
    from source
)

select * from cleaned