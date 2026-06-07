with cours as (
    select * from {{ ref('stg_cours') }}
),

indicateurs as (
    select * from {{ ref('stg_indicateurs') }}
),

final as (
    select
        c.ticker,
        case c.ticker
            when 'BNP.PA' then 'BNP Paribas'
            when 'GLE.PA' then 'Societe Generale'
            when 'ACA.PA' then 'Credit Agricole'
        end as bank_name,
        c.date_cotation,
        c.open_price,
        c.high_price,
        c.low_price,
        c.close_price,
        c.volume,
        round(
            (c.close_price - lag(c.close_price) over (
                partition by c.ticker order by c.date_cotation
            )) / nullif(lag(c.close_price) over (
                partition by c.ticker order by c.date_cotation
            ), 0) * 100,
        2) as variation_pct,
        i.per_ratio,
        i.market_cap,
        i.eps,
        i.dividend_yield,
        i.roe
    from cours c
    left join indicateurs i
        on c.ticker = i.ticker
)

select * from final