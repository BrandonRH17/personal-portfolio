    with 
    
    cmp_acquisition as (
      SELECT 
        campaign_id, 
        count(distinct(click_id)) q_clicks,
        count(distinct(customer_id)) q_clicked_users,
        avg(cost_usd) avg_cost_per_click
      FROM {{ ref('stg_ga__ad_clicks') }} adc
      GROUP BY 1
    ),

    cmp_conversions as (
    SELECT 
      campaign_id,
      count(distinct(conv.customer_id)) q_converted_users, 
      sum(revenue_usd) total_revenue_generated
    FROM {{ ref('stg_ga__conversions') }} conv 
    GROUP BY 1
) 

    SELECT 
      camp.campaign_id,
      camp.campaign_name,
      camp.channel,
      camp.start_date,
      camp.budget_usd,
      camp.status,
      camp.end_date,
      q_clicks, 
      q_clicked_users, 
      ROUND(avg_cost_per_click, 2) as avg_cost_per_click, 

      q_converted_users, 
      ROUND(budget_usd / nullif(q_converted_users, 0), 2) as avg_cost_per_conversion,
      total_revenue_generated
    FROM {{ ref('stg_ga__campaigns') }} camp
    LEFT JOIN cmp_acquisition acq
      on camp.campaign_id = acq.campaign_id
    LEFT JOIN cmp_conversions conv
      on camp.campaign_id = conv.campaign_id
    ORDER BY 
      camp.campaign_id
