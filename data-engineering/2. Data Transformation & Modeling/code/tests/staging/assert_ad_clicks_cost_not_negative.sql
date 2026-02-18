select 
    click_id
from {{ ref('stg_ga__ad_clicks') }}
where
    cost_usd < 0