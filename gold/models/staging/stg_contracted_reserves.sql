-- Reserva de balance contratada por área de control, producto, sentido e intervalo publicado.
--   * Un TSO puede publicar la misma cantidad con dos precios (medio y marginal): se toma la cantidad una vez y
--     el precio medio si existe, si no el marginal.
--   * Después se suman las series del mismo producto y sentido (tipos de recurso o productos de origen distintos).
--   * Se conserva la resolución publicada (15, 30, 60 min o diaria) y las horas que cubre cada fila.
with por_serie as (
    select
        area, product, direction, psr_type, standard_product, original_product,
        interval_start_utc, market_date, resolution,
        max(contracted_mw)                                                         as contracted_mw,
        coalesce(max(procurement_price) filter (where price_category = 'average'),
                 max(procurement_price))                                           as procurement_price,
        any_value(currency)                                                        as currency
    from {{ silver('entsoe_contracted_reserves') }}
    where contracted_mw is not null     -- algún día suelto trae precio sin cantidad (NL FCR, 08/03/2024): no sirve
    group by all
)
select
    area, product, direction, interval_start_utc,
    any_value(market_date)                                                as market_date,
    resolution,
    {{ horas('resolution') }}                                             as hours,
    sum(contracted_mw)                                                    as contracted_mw,
    sum(procurement_price * contracted_mw) / nullif(sum(contracted_mw), 0) as procurement_price,
    any_value(currency)                                                   as currency
from por_serie
group by area, product, direction, interval_start_utc, resolution
