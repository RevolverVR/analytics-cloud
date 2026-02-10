{{ config(materialized='view') }}

{#-
  SLA configurable por prioridad (minutos).
  Defaults (ajústalos en dbt_project.yml vars):
    Alto: 60
    Medio: 120
    Bajo: 240
    Normal: 180
    Not Assigned: 240
-#}

with base as (
    select *
    from {{ ref('stg_base_tickets_typed') }}
),

policy as (
    select
      '{{ var("sla_high_mins", 60) }}'::int        as sla_high_mins,
      '{{ var("sla_medium_mins", 120) }}'::int     as sla_medium_mins,
      '{{ var("sla_low_mins", 240) }}'::int        as sla_low_mins,
      '{{ var("sla_normal_mins", 180) }}'::int     as sla_normal_mins,
      '{{ var("sla_unassigned_mins", 240) }}'::int as sla_unassigned_mins
),

scored as (
    select
        b.*,

        case
            when lower(trim(priority)) = 'alto'         then (select sla_high_mins from policy)
            when lower(trim(priority)) = 'medio'        then (select sla_medium_mins from policy)
            when lower(trim(priority)) = 'bajo'         then (select sla_low_mins from policy)
            when lower(trim(priority)) = 'normal'       then (select sla_normal_mins from policy)
            when lower(trim(priority)) = 'not assigned' then (select sla_unassigned_mins from policy)
            else null
        end as sla_threshold_minutes,

        case
            when b.first_response_minutes is null then null
            when
                case
                    when lower(trim(priority)) = 'alto'         then b.first_response_minutes > (select sla_high_mins from policy)
                    when lower(trim(priority)) = 'medio'        then b.first_response_minutes > (select sla_medium_mins from policy)
                    when lower(trim(priority)) = 'bajo'         then b.first_response_minutes > (select sla_low_mins from policy)
                    when lower(trim(priority)) = 'normal'       then b.first_response_minutes > (select sla_normal_mins from policy)
                    when lower(trim(priority)) = 'not assigned' then b.first_response_minutes > (select sla_unassigned_mins from policy)
                    else null
                end
            then true
            when
                case
                    when lower(trim(priority)) = 'alto'         then b.first_response_minutes <= (select sla_high_mins from policy)
                    when lower(trim(priority)) = 'medio'        then b.first_response_minutes <= (select sla_medium_mins from policy)
                    when lower(trim(priority)) = 'bajo'         then b.first_response_minutes <= (select sla_low_mins from policy)
                    when lower(trim(priority)) = 'normal'       then b.first_response_minutes <= (select sla_normal_mins from policy)
                    when lower(trim(priority)) = 'not assigned' then b.first_response_minutes <= (select sla_unassigned_mins from policy)
                    else null
                end
            then false
            else null
        end as sla_breach
    from base b
)

select * from scored
