-- Test : aucun prix ne doit être nul ou négatif
-- Si ce test retourne des lignes, il échoue

select
    ticker,
    date_cotation,
    close_price,
    open_price,
    high_price,
    low_price
from {{ ref('stg_cours') }}
where
    close_price <= 0
    or open_price <= 0
    or high_price <= 0
    or low_price  <= 0