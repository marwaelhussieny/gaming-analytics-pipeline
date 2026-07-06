-- Intermediate: roll up raw fetches (multiple per day) into one row per
-- game per day, with daily peak/avg. This is the layer that turns
-- "a bunch of API pulls" into something analysis-ready.

with staged as (

    select * from {{ ref('stg_player_counts') }}

),

daily as (

    select
        app_id,
        game_name,
        date_trunc('day', fetched_at)  as report_date,
        max(player_count)              as peak_player_count,
        avg(player_count)              as avg_player_count,
        min(player_count)              as min_player_count,
        count(*)                       as fetch_count
    from staged
    group by 1, 2, 3

)

select * from daily
