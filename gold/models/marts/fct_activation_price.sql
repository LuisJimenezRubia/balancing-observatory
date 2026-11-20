-- Precio de la energía de balance activada por área de control, producto, proceso, sentido, categoría de precio
-- y día de mercado. Solo hay precio en los intervalos con activación: priced_intervals dice cuántos fueron.
select
    area, product, process_type, direction, price_category, market_date,
    date_trunc('month', market_date)::date     as month,
    avg(activation_price_eur_mwh)              as mean_price_eur_mwh,
    min(activation_price_eur_mwh)              as min_price_eur_mwh,
    max(activation_price_eur_mwh)              as max_price_eur_mwh,
    count(*)                                   as priced_intervals,
    any_value(source_currency)                 as source_currency
from {{ ref('stg_activation_price') }}
group by area, product, process_type, direction, price_category, market_date
