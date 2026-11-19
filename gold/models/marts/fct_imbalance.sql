-- Precio y volumen de desvío por área de precio y cuarto de hora.
--   * Grano: el área de precio de desvío. Alemania es una sola (reBAP): su volumen es la suma de las cuatro
--     áreas de control.
--   * imbalance_price_eur_mwh es la media de los dos lados; donde hay precio único coinciden. is_dual_price marca
--     los intervalos en que difieren (Francia casi siempre, Países Bajos a menudo).
--   * system_imbalance_mwh puede faltar (Dinamarca desde jul 2025, Países Bajos antes de 2025): se deja nulo.
with volumen as (
    select
        a.imbalance_price_area as area,
        v.interval_start_utc,
        sum(v.system_imbalance_mwh) as system_imbalance_mwh,
        count(*) as control_areas_reporting
    from {{ ref('stg_imbalance_volume') }} v
    join {{ ref('areas') }} a on a.area = v.area
    group by 1, 2
)
select
    p.area,
    p.interval_start_utc,
    {{ hora_bruselas('p.interval_start_utc') }}                       as interval_start_local,
    p.market_date,
    date_trunc('month', p.market_date)::date                          as month,
    hour({{ hora_bruselas('p.interval_start_utc') }})                 as hour_local,
    p.source_resolution,
    p.source_currency,
    p.price_surplus_eur_mwh,
    p.price_deficit_eur_mwh,
    (coalesce(p.price_surplus_eur_mwh, p.price_deficit_eur_mwh)
     + coalesce(p.price_deficit_eur_mwh, p.price_surplus_eur_mwh)) / 2  as imbalance_price_eur_mwh,
    coalesce(abs(p.price_surplus_eur_mwh - p.price_deficit_eur_mwh) > 0.005, false) as is_dual_price,
    v.system_imbalance_mwh,
    v.control_areas_reporting
from {{ ref('stg_imbalance_price') }} p
left join volumen v on v.area = p.area and v.interval_start_utc = p.interval_start_utc
