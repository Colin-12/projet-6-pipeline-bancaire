-- Test : chaque jour doit avoir les 3 tickers

select
    date_cotation,
    count(distinct ticker) as n_tickers
from {{ ref('stg_cours') }}
group by date_cotation
having count(distinct ticker) < 3