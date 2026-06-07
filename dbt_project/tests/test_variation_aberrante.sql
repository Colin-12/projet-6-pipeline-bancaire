-- Test : aucune variation journalière ne doit dépasser 25%
-- Une variation > 25% en une journée est probablement une erreur de données

select
    ticker,
    date_cotation,
    variation_pct
from {{ ref('mart_bank_daily_kpis') }}
where abs(variation_pct) > 25
  and variation_pct is not null