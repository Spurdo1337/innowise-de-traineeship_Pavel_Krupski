with payment as (

    select * from {{ ref('stg_payment') }}

),

rental as (

    select * from {{ ref('stg_rental') }}

),

inventory as (

    select * from {{ ref('stg_inventory') }}

),

staff as (

    select * from {{ ref('stg_staff') }}

)

select
    payment.payment_id,
    payment.payment_date::date as payment_date,
    payment.payment_date as payment_at,
    payment.customer_id,
    payment.staff_id,
    staff.store_id,
    inventory.film_id,
    payment.amount,
    1 as payment_count
from payment
left join rental
    on payment.rental_id = rental.rental_id
left join inventory
    on rental.inventory_id = inventory.inventory_id
left join staff
    on payment.staff_id = staff.staff_id
