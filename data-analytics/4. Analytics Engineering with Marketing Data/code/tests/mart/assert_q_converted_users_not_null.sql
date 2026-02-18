select 
    campaign_id
from {{ ref('mrt_ga__campaign_performance') }}
where
    q_converted_users < 0