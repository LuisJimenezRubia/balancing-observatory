-- Precio de la energía de balance activada por área de control, producto, proceso (A16 realizado, A68 mercado
-- local), sentido, categoría de precio e intervalo, en euros (las monedas locales se convierten con el tipo BCE).
select
    p.area, p.product, p.process_type, p.direction, p.price_category, p.interval_start_utc,
    any_value(p.market_date)                                              as market_date,
    any_value(p.resolution)                                               as resolution,
    any_value(p.currency)                                                 as source_currency,
    avg(case when p.currency = 'EUR' then p.activation_price
             else p.activation_price / fx.units_per_eur end)              as activation_price_eur_mwh
from {{ silver('entsoe_activation_price') }} p
asof left join {{ ref('stg_fx_rate') }} fx
    on fx.currency = p.currency and fx.rate_date <= p.market_date
group by p.area, p.product, p.process_type, p.direction, p.price_category, p.interval_start_utc
