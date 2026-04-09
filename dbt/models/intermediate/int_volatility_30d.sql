-- 30 day rolling volatility per ticker
-- measured as standard deviation of daily return %

with returns as (

    select * from {{ ref('int_daily_returns') }}

),

final as (

    select
        ticker,
        price_date,
        daily_return_pct,
        round(
            stddev(daily_return_pct) over (
                partition by ticker
                order by price_date
                rows between 29 preceding and current row
            ),
        4) as volatility_30d

    from returns

)

select * from final