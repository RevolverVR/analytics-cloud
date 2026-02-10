{{ config(materialized='view') }}

with base as (
    select
        request_id,
        reopened_bool,
        first_response_minutes,
        priority,
        request_status,
        created_ts,
        responded_ts
    from {{ ref('stg_base_tickets_typed') }}
),

agg as (
    select
        request_id,

        -- si cualquier fila indica reapertura → no FCR
        bool_or(coalesce(reopened_bool, false)) as was_reopened,

        min(created_ts) as created_ts,
        min(responded_ts) as first_responded_ts,
        min(first_response_minutes) as first_response_minutes,

        any_value(priority) as priority,
        any_value(request_status) as request_status
    from base
    group by 1
)

select
    *,
    case
        when was_reopened = false then true
        when was_reopened = true then false
        else null
    end as fcr_proxy
from agg
