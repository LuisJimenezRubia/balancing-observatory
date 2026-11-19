-- Perfil por hora del día (hora de Bruselas) del error de previsión, por zona y tecnología, sobre todo el periodo.
select
    area,
    technology,
    hour_local,
    any_value(is_comparable)                          as is_comparable,
    any_value(comparability_note)                     as comparability_note,
    count(*)                                          as hours,
    avg(actual_mw)                                    as mean_actual_mw,
    avg(abs_error_mw)                                 as mae_mw,
    avg(error_mw)                                     as bias_mw,
    avg(abs_error_mw) / nullif(avg(actual_mw), 0)     as nmae
from {{ ref('fct_forecast_error') }}
group by area, technology, hour_local
