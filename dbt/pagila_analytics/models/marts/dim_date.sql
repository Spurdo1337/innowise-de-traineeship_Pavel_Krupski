-- Calendar dimension built from the date spine variables in dbt_project.yml.
-- Pagila has no calendar table, so the range is generated explicitly.

{% set start = modules.datetime.date.fromisoformat(var('date_spine_start')) %}
{% set end = modules.datetime.date.fromisoformat(var('date_spine_end')) %}
{% set days = (end - start).days + 1 %}

with date_spine as (

    select
        dateadd(day, seq4(), to_date('{{ var("date_spine_start") }}')) as date_day
    from table(generator(rowcount => {{ days }}))

)

select
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    to_char(date_day, 'MMMM') as month_name,
    extract(day from date_day) as day_of_month,
    dayofweekiso(date_day) as day_of_week,
    to_char(date_day, 'DY') as day_name,
    (dayofweekiso(date_day) in (6, 7)) as is_weekend
from date_spine
