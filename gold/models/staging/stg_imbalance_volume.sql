-- Desvío del sistema a 15 min por área de control, con signo: positivo = sistema largo (excedente),
-- negativo = sistema corto (déficit).
--   * ENTSO-E publica dos series por sentido. Comprobado con los datos: los intervalos de la serie A02 ("down")
--     coinciden con los precios de desvío altos, es decir, con el sistema corto; los de A01 ("up"), con el largo.
--   * Los periodos de 30 y 60 min se reparten a partes iguales entre sus cuartos de hora (es energía, MWh).
with expandido as (
    select
        v.area,
        v.interval_start_utc + (k * interval 15 minute) as interval_start_utc,
        v.market_date,
        v.resolution,
        case v.direction when 'up' then 1 when 'down' then -1 else 0 end
            * v.imbalance_mwh / {{ cuartos('v.resolution') }} as imbalance_mwh
    from {{ silver('entsoe_imbalance_volume') }} v,
         generate_series(0, {{ cuartos('v.resolution') }} - 1) as g(k)
    qualify dense_rank() over (
        partition by v.area, v.interval_start_utc + (k * interval 15 minute)
        order by {{ cuartos('v.resolution') }}
    ) = 1
)
select
    area,
    interval_start_utc,
    any_value(market_date)  as market_date,
    any_value(resolution)   as source_resolution,
    sum(imbalance_mwh)      as system_imbalance_mwh
from expandido
group by area, interval_start_utc
