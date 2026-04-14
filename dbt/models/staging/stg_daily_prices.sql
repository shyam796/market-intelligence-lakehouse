-- clean and cast daily price data from bronze
-- deduplicate on ticker + price_date keeping the latest ingested row
-- test ci pipeline
with source as (

    select * from {{ source('bronze', 'daily_prices') }}

),

deduplicated as (

    select
        ticker,
        price_date,
        open,
        high,
        low,
        close,
        volume,
        ingested_at,
        ingestion_date,
        row_number() over (
            partition by ticker, price_date
            order by ingested_at desc
        ) as row_num

    from source

),

final as (

    select
        ticker,
        price_date,
        cast(open   as decimal(10, 4)) as open,
        cast(high   as decimal(10, 4)) as high,
        cast(low    as decimal(10, 4)) as low,
        cast(close  as decimal(10, 4)) as close,
        cast(volume as bigint)         as volume,
        ingested_at,
        ingestion_date
    from deduplicated
    where row_num = 1

)

select * from final