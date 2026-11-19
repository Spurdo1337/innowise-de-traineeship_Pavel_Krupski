with customer as (

    select * from {{ ref('stg_customer') }}

),

address as (

    select * from {{ ref('stg_address') }}

),

city as (

    select * from {{ ref('stg_city') }}

),

country as (

    select * from {{ ref('stg_country') }}

)

select
    customer.customer_id,
    customer.store_id,
    customer.first_name,
    customer.last_name,
    customer.email,
    customer.activebool,
    customer.active,
    customer.create_date,
    address.address,
    address.district,
    address.postal_code,
    city.city_id,
    city.city,
    country.country
from customer
inner join address
    on customer.address_id = address.address_id
inner join city
    on address.city_id = city.city_id
inner join country
    on city.country_id = country.country_id
