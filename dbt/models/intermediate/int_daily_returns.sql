-- calculates daily return % for each ticker
-- return = (today's close - yesterday's close) / yesterday's close

with prices as (

    select * from {{ ref('stg_daily_prices') }}

),

final as (

    select
        ticker,
        price_date,
        close,
        lag(close) over (
            partition by ticker
            order by price_date
        ) as prev_close,

        round(
            (close - lag(close) over (
                partition by ticker
                order by price_date
            )) / nullif(lag(close) over (
                partition by ticker
                order by price_date
            ), 0) * 100,
        4) as daily_return_pct

    from prices

)

select * from final