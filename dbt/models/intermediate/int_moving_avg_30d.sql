-- 30 day rolling average close price per ticker

with prices as (

    select * from {{ ref('stg_daily_prices') }}

),

final as (

    select
        ticker,
        price_date,
        close,
        round(
            avg(close) over (
                partition by ticker
                order by price_date
                rows between 29 preceding and current row
            ),
        4) as moving_avg_30d

    from prices

)

select * from final