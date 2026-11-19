-- Convergencia del CBMP de cada área con Alemania (50Hertz), contada ciclo a ciclo de 4 s en silver.
select
    a.imbalance_price_area as area,
    r.imbalance_price_area as reference_area,
    c.direction,
    c.interval_start_utc,
    c.market_date,
    c.cycles_total,
    c.cycles_both_priced,
    c.cycles_equal_price,
    c.cycles_only_area,
    c.cycles_only_reference,
    c.mean_spread_eur_mwh,
    c.mean_abs_spread_eur_mwh,
    c.max_abs_spread_eur_mwh
from {{ silver('entsoe_cbmp_convergence_15min') }} c
join {{ ref('areas') }} a on a.area = c.area
join {{ ref('areas') }} r on r.area = c.reference_area
