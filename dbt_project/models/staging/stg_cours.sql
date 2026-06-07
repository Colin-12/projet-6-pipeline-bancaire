-- stg_cours.sql
-- Staging des cours boursiers
-- Dédoublonnage : on garde la ligne la plus récente par (ticker, date)
-- Grain : 1 ticker × 1 date de cotation

with source as (
    select * from {{ source('projet6_raw', 'raw_market_data') }}
),

deduped as (
    select *,
        row_number() over (
            partition by ticker, date_cotation
            order by _ingested_at desc
        ) as rn
    from source
),

final as (
    select
        ticker,
        date_cotation,
        open        as open_price,
        high        as high_price,
        low         as low_price,
        close       as close_price,
        adj_close,
        volume,
        _ingested_at
    from deduped
    where rn = 1
)

select * from final