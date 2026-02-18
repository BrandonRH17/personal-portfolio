with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select 
        customer_id, 
        {{ proper_case('first_name') }} as first_name,
        {{ proper_case('last_name') }} as last_name,
        {{ proper_case('first_name') }} || {{ proper_case('last_name') }} as full_name, 
        {{ clean_email('email') }} as email, 
        {{ proper_case('country') }} as country, 
        cast(signup_date as date) as signup_date
    from source
)

select * 
from cleaned