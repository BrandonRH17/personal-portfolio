select 
    campaign_id
from {{ref('mrt_ga__campaign_performance')}}
where
    avg_cost_per_conversion < 0