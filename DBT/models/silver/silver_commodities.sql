-- import 

with source as (
    select 
    "Date",
    "Close",
    "ticker"
    from {{source ('databasedbt', 'commodities')}}
),


-- renamed

renamed as (
    select
    cast("Date" as date) as data_fechamento,
    "Close" as valor_fechamento,
    "ticker"
    from source
)


-- select * from
select * from renamed 