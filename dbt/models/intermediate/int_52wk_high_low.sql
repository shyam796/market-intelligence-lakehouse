-- 52 week high and low close price per ticker

with prices as (

    select * from {{ ref('stg_daily_prices') }}

),

final as (

    select
        ticker,
        price_date,
        close,
        max(close) over (
            partition by ticker
            order by price_date
            rows between 364 preceding and current row
        ) as high_52wk,

        min(close) over (
            partition by ticker
            order by price_date
            rows between 364 preceding and current row
        ) as low_52wk

    from prices

)

select * from final