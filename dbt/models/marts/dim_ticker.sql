-- one row per ticker with summary stats
-- slowly changing dimension, rebuilt daily as a table

with tickers as (

    select * from {{ ref('stg_tickers') }}

),

final as (

    select
        ticker,
        first_seen_date,
        last_seen_date,
        total_trading_days,
        current_timestamp() as updated_at

    from tickers

)

select * from final