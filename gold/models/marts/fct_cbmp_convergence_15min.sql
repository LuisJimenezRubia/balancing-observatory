-- Convergencia del CBMP con el área de referencia por cuarto de hora.
-- convergence_share = ciclos con precio idéntico / ciclos en que las dos áreas tienen precio. Es nula si en el
-- cuarto de hora no coincidieron con precio en ningún ciclo: ahí no hay nada que comparar.
select
    area,
    reference_area,
    direction,
    interval_start_utc,
    {{ hora_bruselas('interval_start_utc') }}           as interval_start_local,
    market_date,
    date_trunc('month', market_date)::date              as month,
    hour({{ hora_bruselas('interval_start_utc') }})     as hour_local,
    cycles_total,
    cycles_both_priced,
    cycles_equal_price,
    cycles_only_area,
    cycles_only_reference,
    cycles_equal_price / nullif(cycles_both_priced, 0)::double                  as convergence_share,
    cycles_both_priced > 0 and cycles_equal_price = cycles_both_priced          as is_fully_converged,
    mean_spread_eur_mwh,
    mean_abs_spread_eur_mwh,
    max_abs_spread_eur_mwh
from {{ ref('stg_cbmp_convergence_15min') }}
