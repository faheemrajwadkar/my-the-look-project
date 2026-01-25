select 
    date_day as date,
    dayname(date_day) as day_name,
    monthname(date_day) as month_name,
    date_trunc('year', date_day) as year,
    date_trunc('quarter', date_day) as quarter,
    date_trunc('month', date_day) as month,
    date_trunc('week', date_day) as week,
    case when dayname(date_day) in ('Sun', 'Sat') then 1 else 0 end as weekend_flag
from (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2018-01-01' as date)",
        end_date="cast(current_date as date)"
    )
    }}
) a 