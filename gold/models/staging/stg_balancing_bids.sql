-- Energía de balance ofertada, activada y no disponible por área de control, producto, sentido e intervalo (MW
-- medios del intervalo). Se suman las series del mismo producto y sentido. La activada queda nula donde el TSO
-- no la publica.
select
    area, product, direction, interval_start_utc,
    any_value(market_date)       as market_date,
    resolution,
    {{ horas('resolution') }}    as hours,
    sum(offered_mw)              as offered_mw,
    sum(activated_mw)            as activated_mw,
    sum(unavailable_mw)          as unavailable_mw
from {{ silver('entsoe_balancing_bids') }}
group by area, product, direction, interval_start_utc, resolution
