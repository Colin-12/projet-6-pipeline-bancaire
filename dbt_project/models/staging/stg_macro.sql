-- stg_macro.sql
-- Staging des taux macro BCE
-- Grain : 1 indicateur × 1 date d'observation

with source as (
    select * from {{ source('projet6_raw', 'raw_macro') }}
),

deduped as (
    select *,
        row_number() over (
            partition by indicator_code, date_observation
            order by _ingested_at desc
        ) as rn
    from source
),

final as (
    select
        indicator_code,
        series_id,
        date_observation,
        value,
        _ingested_at
    from deduped
    where rn = 1
)

select * from final