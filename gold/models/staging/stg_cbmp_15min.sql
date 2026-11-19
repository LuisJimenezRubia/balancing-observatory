-- CBMP de aFRR (PICASSO) por área de precio, sentido y cuarto de hora, agregado en silver desde los ciclos de 4 s.
-- Las tres áreas de control alemanas con dato publican exactamente el mismo CBMP: se toma 50Hertz como Alemania.
select
    a.imbalance_price_area as area,
    c.direction,
    c.interval_start_utc,
    c.market_date,
    c.cbmp_mean_eur_mwh,
    c.cbmp_min_eur_mwh,
    c.cbmp_max_eur_mwh,
    c.cbmp_std_eur_mwh,
    c.cycles_with_price,
    c.cycles_total
from {{ silver('entsoe_cbmp_15min') }} c
join {{ ref('areas') }} a on a.area = c.area
where c.area not in ('DE_TENNET', 'DE_TRANSNETBW')
