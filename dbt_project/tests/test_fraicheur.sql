-- Test : les données ne doivent pas avoir plus de 3 jours de retard

select 1
from (
    select max(date_cotation) as last_date
    from {{ ref('stg_cours') }}
)
where last_date < date_sub(current_date(), interval 3 day)