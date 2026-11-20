-- Reserva contratada por área de control, producto (FCR, aFRR, mFRR, RR), sentido y día de mercado.
--   contracted_mw       media del día ponderada por las horas que cubre cada valor publicado
--   procurement_price   precio de adquisición tal como lo publica el TSO, ponderado por MW y horas. La unidad
--                       temporal (EUR/MW por hora o por periodo contratado) no viene en el dato y puede variar
--                       entre TSO: sirve para ver la evolución de un área, no para comparar áreas sin comprobarla.
select
    area, product, direction, market_date,
    date_trunc('month', market_date)::date                                        as month,
    sum(contracted_mw * hours) / sum(hours)                                       as contracted_mw,
    sum(procurement_price * contracted_mw * hours)
        / nullif(sum(case when procurement_price is not null then contracted_mw * hours end), 0) as procurement_price,
    any_value(currency)                                                           as currency,
    sum(hours)                                                                    as hours_covered,
    string_agg(distinct resolution, ',' order by resolution)                      as source_resolutions
from {{ ref('stg_contracted_reserves') }}
group by area, product, direction, market_date
