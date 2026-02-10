{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_base_tickets') }}
),

typed as (
    select
        -- Fechas (DD/MM confirmado)
        try_strptime(trim(created_time), '%d/%m/%Y %H:%M') as created_ts,

        try_strptime(
            nullif(trim(responded_date), 'Not Assigned'),
            '%d/%m/%Y %H:%M'
        ) as responded_ts,

        -- Boolean normalizado
        case
            when upper(trim(reopened)) in ('TRUE','T','1','YES','Y','SI') then true
            when upper(trim(reopened)) in ('FALSE','F','0','NO','N') then false
            else null
        end as reopened_bool,

        -- Métrica base: minutos a primera respuesta (solo si tiene sentido)
        case
            when
                try_strptime(trim(created_time), '%d/%m/%Y %H:%M') is not null
                and try_strptime(nullif(trim(responded_date), 'Not Assigned'), '%d/%m/%Y %H:%M') is not null
                and try_strptime(nullif(trim(responded_date), 'Not Assigned'), '%d/%m/%Y %H:%M')
                    >= try_strptime(trim(created_time), '%d/%m/%Y %H:%M')
            then date_diff(
                'minute',
                try_strptime(trim(created_time), '%d/%m/%Y %H:%M'),
                try_strptime(nullif(trim(responded_date), 'Not Assigned'), '%d/%m/%Y %H:%M')
            )
            else null
        end as first_response_minutes,

        -- conserva columnas crudas
        *
    from src
)

select *
from typed
