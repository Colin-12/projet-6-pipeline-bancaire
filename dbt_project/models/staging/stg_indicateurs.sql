-- stg_indicateurs.sql
-- Staging des indicateurs fondamentaux
-- Grain : 1 ticker × 1 date d'extraction

with source as (
    select * from {{ source('projet6_raw', 'raw_financials') }}
),

deduped as (
    select *,
        row_number() over (
            partition by ticker, extraction_date
            order by _ingested_at desc
        ) as rn
    from source
),

final as (
    select
        ticker,
        company_name,
        extraction_date,
        cast(market_cap as float64)    as market_cap,
        cast(per_ratio as float64)     as per_ratio,
        cast(eps as float64)           as eps,
        cast(dividend_yield as float64)as dividend_yield,
        cast(price_to_book as float64) as price_to_book,
        cast(roe as float64)           as roe,
        _ingested_at
    from deduped
    where rn = 1
)

select * from final