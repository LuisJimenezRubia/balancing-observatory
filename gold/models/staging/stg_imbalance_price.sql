-- Precio de desvío a 15 min, una fila por área e intervalo, con los dos lados en columnas y ya en euros.
--   * Las resoluciones de 30 y 60 min (FR hasta 2024, CZ hasta 2024, DK hasta 2025) se reparten en sus cuartos de
--     hora: el precio del periodo vale para cada uno. Si un cuarto de hora está en dos resoluciones (meses de
--     transición), gana la más fina.
--   * Chequia publica en coronas: se pasa a euros con el tipo de referencia del BCE del día de mercado o, en fin
--     de semana y festivos, el último publicado.
with expandido as (
    select
        p.area,
        p.imbalance_side,
        p.interval_start_utc + (k * interval 15 minute) as interval_start_utc,
        p.market_date,
        p.resolution,
        p.currency,
        p.imbalance_price
    from {{ silver('entsoe_imbalance_price') }} p,
         generate_series(0, {{ cuartos('p.resolution') }} - 1) as g(k)
    qualify row_number() over (
        partition by p.area, p.imbalance_side, p.interval_start_utc + (k * interval 15 minute)
        order by {{ cuartos('p.resolution') }}
    ) = 1
),
en_euros as (
    select
        e.* exclude (imbalance_price, currency),
        e.currency as source_currency,
        case when e.currency = 'EUR' then e.imbalance_price else e.imbalance_price / fx.units_per_eur end as price_eur_mwh
    from expandido e
    asof left join {{ ref('stg_fx_rate') }} fx
        on fx.currency = e.currency and fx.rate_date <= e.market_date
)
select
    area,
    interval_start_utc,
    any_value(market_date)                                        as market_date,
    any_value(resolution)                                         as source_resolution,
    any_value(source_currency)                                    as source_currency,
    max(price_eur_mwh) filter (where imbalance_side = 'surplus')  as price_surplus_eur_mwh,
    max(price_eur_mwh) filter (where imbalance_side = 'deficit')  as price_deficit_eur_mwh
from en_euros
group by area, interval_start_utc
