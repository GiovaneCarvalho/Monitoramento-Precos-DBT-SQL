-- models/datamart/dm_commodities.sql

with commodities as (
    select
        data_fechamento,
        ticker,
        valor_fechamento
    from 
        {{ ref ('silver_commodities') }}
),

movimentacao as (
    select
        data,
        simbolo,
        acao,
        quantidade
    from 
        {{ ref ('silver_mov_commodities') }}
),

joined as (
    select
        c.data_fechamento as data,
        c.ticker as simbolo,
        c.valor_fechamento,
        m.acao,
        m.quantidade,
        (m.quantidade * c.valor_fechamento) as valor,
        case
            when m.acao = 'sell' then (m.quantidade * c.valor_fechamento)
            else -(m.quantidade * c.valor_fechamento)
        end as ganho
    from
        commodities c
    inner join
        movimentacao m
    on
        c.data_fechamento = m.data
        and c.ticker = m.simbolo
),

last_day as (
    select
        max(data) as max_date
    from
        joined
),

filtered as (
    select
        *
    from
        joined
    where
        data = (select max_date from last_day)
)

select
    data,
    simbolo,
    valor_fechamento,
    acao,
    quantidade,
    valor,
    ganho
from
    filtered