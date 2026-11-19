-- The calendar must cover exactly the range configured in dbt_project.yml.

with bounds as (

    select
        min(date_day) as min_day,
        max(date_day) as max_day,
        count(*) as day_count
    from {{ ref('dim_date') }}

)

select *
from bounds
where min_day != to_date('{{ var("date_spine_start") }}')
   or max_day != to_date('{{ var("date_spine_end") }}')
   or day_count != datediff(
        day,
        to_date('{{ var("date_spine_start") }}'),
        to_date('{{ var("date_spine_end") }}')
   ) + 1
