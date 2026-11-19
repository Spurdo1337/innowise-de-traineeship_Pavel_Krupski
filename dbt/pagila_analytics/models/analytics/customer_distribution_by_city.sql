-- Stage 1, task 6: active and inactive customers per city.

with city as (

    select * from {{ ref('stg_city') }}

),

address as (

    select * from {{ ref('stg_address') }}

),

customer as (

    select * from {{ ref('stg_customer') }}

)

select
    city.city,
    sum(case when customer.active = 1 then 1 else 0 end) as active_customers,
    sum(case when customer.active = 0 then 1 else 0 end) as inactive_customers
from city
left join address
    on address.city_id = city.city_id
left join customer
    on customer.address_id = address.address_id
group by city.city_id, city.city
order by inactive_customers desc, city.city
