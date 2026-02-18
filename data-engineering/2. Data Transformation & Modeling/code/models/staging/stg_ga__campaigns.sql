with source as (
    select * from {{ source('raw', 'campaigns') }}
), 

cleaned as (
     SELECT 
        campaign_id, 
        campaign_name, 
        channel, 
        cast(start_date as date) start_date, 
        round(budget_usd, 2) as budget_usd, 
        status, 
        cast(end_date as date) end_date
    from source
)

select * from source
