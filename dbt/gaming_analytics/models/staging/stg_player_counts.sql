-- Staging: flatten the raw VARIANT JSON loaded from Steam API into typed columns.
-- One row per (app_id, fetched_at) — this is the atomic grain for everything downstream.

with source as (

    select raw_payload
    from {{ source('raw', 'player_counts') }}

),

flattened as (

    select
        raw_payload:app_id::int            as app_id,
        raw_payload:game_name::string      as game_name,
        raw_payload:player_count::int      as player_count,
        raw_payload:fetched_at::timestamp  as fetched_at
    from source

)

select * from flattened
where player_count is not null  -- drop failed fetches at the staging boundary
