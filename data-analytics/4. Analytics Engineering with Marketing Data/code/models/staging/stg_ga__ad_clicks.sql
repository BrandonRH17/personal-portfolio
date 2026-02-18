with source as (
    select * from {{ source('raw', 'ad_clicks') }}
), 

cleaned as (
    select 
      click_id, 
      campaign_id, 
      customer_id, 
      cast(click_date as date) as click_date, 
      lower(device) as device, 
      round(cost_usd, 2) as cost_usd
    from source 
)

select * from cleaned