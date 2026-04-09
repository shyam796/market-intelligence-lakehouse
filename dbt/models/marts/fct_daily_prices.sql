-- one row per ticker per trading day
-- enriched with return %, moving average, volatility, 52wk high/low

with prices as (

    select * from {{ ref('stg_daily_prices') }}

),

returns as (

    select
        ticker,
        price_date,
        daily_return_pct
    from {{ ref('int_daily_returns') }}

),

moving_avg as (

    select
        ticker,
        price_date,
        moving_avg_30d
    from {{ ref('int_moving_avg_30d') }}

),

volatility as (

    select
        ticker,
        price_date,
        volatility_30d
    from {{ ref('int_volatility_30d') }}

),

high_low as (

    select
        ticker,
        price_date,
        high_52wk,
        low_52wk
    from {{ ref('int_52wk_high_low') }}

),

final as (

    select
        prices.ticker,
        prices.price_date,
        prices.open,
        prices.high,
        prices.low,
        prices.close,
        prices.volume,
        returns.daily_return_pct,
        moving_avg.moving_avg_30d,
        volatility.volatility_30d,
        high_low.high_52wk,
        high_low.low_52wk,
        prices.ingested_at

    from prices
    left join returns
        on prices.ticker = returns.ticker
        and prices.price_date = returns.price_date
    left join moving_avg
        on prices.ticker = moving_avg.ticker
        and prices.price_date = moving_avg.price_date
    left join volatility
        on prices.ticker = volatility.ticker
        and prices.price_date = volatility.price_date
    left join high_low
        on prices.ticker = high_low.ticker
        and prices.price_date = high_low.price_date

)

select * from final