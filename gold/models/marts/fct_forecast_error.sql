-- Error de la previsión day-ahead por zona, tecnología (wind_onshore, wind_offshore, solar, load) y hora.
-- error_mw = real − previsión: positivo si hubo más generación (o más carga) de la prevista.
-- Solo entran las horas en que previsión y real cubren los cuatro cuartos de hora.
-- is_comparable = false marca las series en que previsión y real no cubren el mismo perímetro en la fuente
-- (seed forecast_comparability): se conservan para enseñarlas en la página de calidad de datos, pero no deben
-- entrar en el benchmark.
select
    s.area,
    s.technology,
    coalesce(c.is_comparable, true)                          as is_comparable,
    c.reason                                                 as comparability_note,
    hour_start_utc,
    {{ hora_bruselas('hour_start_utc') }}                    as hour_start_local,
    cast({{ hora_bruselas('hour_start_utc') }} as date)      as market_date,
    date_trunc('month', {{ hora_bruselas('hour_start_utc') }})::date as month,
    hour({{ hora_bruselas('hour_start_utc') }})              as hour_local,
    forecast_mw,
    actual_mw,
    actual_mw - forecast_mw                                  as error_mw,
    abs(actual_mw - forecast_mw)                             as abs_error_mw
from {{ ref('stg_forecast_actual_hourly') }} s
left join {{ ref('forecast_comparability') }} c on c.area = s.area and c.technology = s.technology
where forecast_quarters = 4 and actual_quarters = 4
