-- pull distinct tickers from bronze price data
-- one row per ticker with first and last seen dates

with source as (

    select * from {{ source('bronze', 'daily_prices') }}

),

final as (

    select
        ticker,
        min(price_date) as first_seen_date,
        max(price_date) as last_seen_date,
        count(*)        as total_trading_days
    from source
    group by ticker

)

select * from final