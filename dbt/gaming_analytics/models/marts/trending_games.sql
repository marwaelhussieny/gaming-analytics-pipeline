-- Mart: day-over-day trend for each game, ready for BI/dashboard consumption.
-- This is the table your Grafana/Streamlit dashboard would query directly.

with daily_trends as (

    select * from {{ ref('int_daily_player_trends') }}

),

with_previous_day as (

    select
        *,
        lag(peak_player_count) over (
            partition by app_id order by report_date
        ) as prev_day_peak_player_count
    from daily_trends

)

select
    app_id,
    game_name,
    report_date,
    peak_player_count,
    avg_player_count,
    min_player_count,
    prev_day_peak_player_count,
    peak_player_count - prev_day_peak_player_count as day_over_day_change,
    case
        when prev_day_peak_player_count is null or prev_day_peak_player_count = 0 then null
        else round(
            100.0 * (peak_player_count - prev_day_peak_player_count) / prev_day_peak_player_count,
            2
        )
    end as day_over_day_pct_change
from with_previous_day
order by report_date desc, peak_player_count desc
