{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'base_tickets') }}
)

select *
from source
