-- Resumen mensual por área de control, producto y sentido: reserva contratada, energía ofertada y activada, y
-- precio medio de activación. Una fila aunque falte alguno de los cuatro datos (queda nulo).
with reservas as (
    select area, product, direction, month,
           avg(contracted_mw) as contracted_mw, avg(procurement_price) as procurement_price
    from {{ ref('fct_contracted_reserves') }} group by all
),
energia as (
    select area, product, direction, month,
           avg(offered_mw_avg) as offered_mw_avg, sum(activated_mwh) as activated_mwh,
           avg(activated_isp_share) as activated_isp_share
    from {{ ref('fct_balancing_energy') }} group by all
),
precio as (
    -- precio de activación: categoría "average" si existe en el mes, si no la marginal; procesos A16 y A68 juntos
    select area, product, direction, month,
           coalesce(sum(mean_price_eur_mwh * priced_intervals) filter (where price_category = 'average')
                        / nullif(sum(priced_intervals) filter (where price_category = 'average'), 0),
                    sum(mean_price_eur_mwh * priced_intervals) filter (where price_category = 'marginal')
                        / nullif(sum(priced_intervals) filter (where price_category = 'marginal'), 0)) as activation_price_eur_mwh,
           sum(priced_intervals) as priced_intervals
    from {{ ref('fct_activation_price') }} group by all
)
select
    coalesce(r.area, e.area, p.area)                as area,
    coalesce(r.product, e.product, p.product)       as product,
    coalesce(r.direction, e.direction, p.direction) as direction,
    coalesce(r.month, e.month, p.month)             as month,
    r.contracted_mw,
    r.procurement_price,
    e.offered_mw_avg,
    e.activated_mwh,
    e.activated_isp_share,
    p.activation_price_eur_mwh,
    p.priced_intervals
from reservas r
full join energia e using (area, product, direction, month)
full join precio p
    on  p.area = coalesce(r.area, e.area) and p.product = coalesce(r.product, e.product)
    and p.direction = coalesce(r.direction, e.direction) and p.month = coalesce(r.month, e.month)
