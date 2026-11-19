-- CBMP de aFRR por área, sentido (up = al alza, down = a la baja) y cuarto de hora.
-- La media, el mínimo, el máximo y la desviación son sobre los ciclos de 4 s con precio; activation_share dice
-- qué parte de los ciclos del cuarto de hora lo tenía (sin precio = sin activación en ese sentido).
select
    area,
    direction,
    interval_start_utc,
    {{ hora_bruselas('interval_start_utc') }}           as interval_start_local,
    market_date,
    date_trunc('month', market_date)::date              as month,
    hour({{ hora_bruselas('interval_start_utc') }})     as hour_local,
    cbmp_mean_eur_mwh,
    cbmp_min_eur_mwh,
    cbmp_max_eur_mwh,
    cbmp_std_eur_mwh,
    cycles_with_price,
    cycles_total,
    cycles_with_price / cycles_total::double            as activation_share
from {{ ref('stg_cbmp_15min') }}
