with customer as (

    select * from {{ ref('int_customer_enriched') }}

)

select
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    activebool,
    active,
    address,
    district,
    postal_code,
    city_id,
    city,
    country,
    create_date
from customer
