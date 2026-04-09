-- fails if any price_date is in the future
-- catches bad data coming through from the api

select *
from {{ ref('fct_daily_prices') }}
where price_date > current_date()