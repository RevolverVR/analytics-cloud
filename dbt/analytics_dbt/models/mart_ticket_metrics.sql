{{ config(materialized='table') }}

with fcr as (
    select *
    from {{ ref('fcr_ticket') }}
),

sla as (
    select
        request_id,
        sla_breach,
        sla_threshold_minutes
    from {{ ref('sla_first_response') }}
),

joined as (
    select
        f.request_id,
        f.created_ts,
        f.first_responded_ts,
        f.first_response_minutes,
        f.priority,
        f.request_status,
        f.fcr_proxy,
        s.sla_breach,
        s.sla_threshold_minutes
    from fcr f
    left join sla s using (request_id)
)

select * from joined
