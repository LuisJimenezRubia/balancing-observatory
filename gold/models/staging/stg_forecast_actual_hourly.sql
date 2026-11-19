-- Previsión day-ahead y valor real de eólica, solar y carga, por zona, tecnología y hora (UTC), en MW.
-- Las zonas publican a 15 o a 60 min según el dato (la previsión renovable de BE y CZ y la carga de DK son
-- horarias). Para comparar todas con la misma vara se lleva todo a la hora: media de los valores de la hora
-- (es potencia, no energía). Se guarda cuántos cuartos de hora cubre cada lado para poder exigir horas completas.
with medidas as (
    select area, technology, interval_start_utc, resolution, 'forecast' as kind, forecast_mw as mw
    from {{ silver('entsoe_wind_solar_forecast') }}
    union all
    select area, technology, interval_start_utc, resolution, 'actual', actual_mw
    from {{ silver('entsoe_wind_solar_actual') }}
    union all
    select area, 'load', interval_start_utc, resolution, 'forecast', load_forecast_mw
    from {{ silver('entsoe_load_forecast') }}
    union all
    select area, 'load', interval_start_utc, resolution, 'actual', load_mw
    from {{ silver('entsoe_load_actual') }}
)
select
    area,
    technology,
    date_trunc('hour', interval_start_utc)                                           as hour_start_utc,
    avg(mw) filter (where kind = 'forecast')                                         as forecast_mw,
    avg(mw) filter (where kind = 'actual')                                           as actual_mw,
    coalesce(sum({{ cuartos('resolution') }}) filter (where kind = 'forecast'), 0)   as forecast_quarters,
    coalesce(sum({{ cuartos('resolution') }}) filter (where kind = 'actual'), 0)     as actual_quarters
from medidas
group by 1, 2, 3
