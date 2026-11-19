-- Puente entre las dos mitades del proyecto: ¿cuánto del desvío del sistema acompaña al error de previsión?
-- Por área de precio y mes, a nivel horario:
--   net_forecast_error_mw = (real − previsto) de eólica y solar − (real − previsto) de carga.
--     Positivo = sobra energía respecto a lo previsto el día antes, lo que empuja al sistema a quedar largo.
--   corr_error_imbalance  correlación con el desvío del sistema (positivo = largo): cuanto más alta, más pesa
--     el error de previsión day-ahead en el desvío. No es causalidad: el intradiario corrige parte del error.
-- Solo horas con todas las tecnologías de la zona y el desvío disponibles. La zona DE_LU se compara con el
-- área de precio DE.
with error_hora as (
    select
        a.imbalance_price_area as area,
        f.hour_start_utc,
        any_value(f.month) as month,
        sum(case when f.technology = 'load' then -f.error_mw else f.error_mw end) as net_forecast_error_mw,
        count(*) as technologies
    from {{ ref('fct_forecast_error') }} f
    join {{ ref('areas') }} a on a.area = f.area
    group by 1, 2
),
tecnologias_zona as (
    select area, max(technologies) as technologies from error_hora group by 1
),
desvio_hora as (
    select
        area,
        date_trunc('hour', interval_start_utc)  as hour_start_utc,
        sum(system_imbalance_mwh)               as system_imbalance_mwh,
        avg(imbalance_price_eur_mwh)            as imbalance_price_eur_mwh,
        count(system_imbalance_mwh)             as quarters
    from {{ ref('fct_imbalance') }}
    group by 1, 2
)
select
    e.area,
    e.month,
    count(*)                                                        as hours,
    corr(e.net_forecast_error_mw, d.system_imbalance_mwh)           as corr_error_imbalance,
    corr(abs(e.net_forecast_error_mw), abs(d.imbalance_price_eur_mwh)) as corr_abs_error_abs_price,
    avg(abs(e.net_forecast_error_mw))                               as mean_abs_net_error_mw,
    avg(abs(d.system_imbalance_mwh))                                as mean_abs_imbalance_mwh
from error_hora e
join tecnologias_zona t on t.area = e.area and t.technologies = e.technologies
join desvio_hora d on d.area = e.area and d.hour_start_utc = e.hour_start_utc and d.quarters = 4
group by e.area, e.month
