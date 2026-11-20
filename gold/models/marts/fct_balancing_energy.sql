-- Energía de balance ofertada y activada por área de control, producto, sentido y día de mercado.
--   offered_mw_avg      media diaria de los MW ofertados (liquidez)
--   activated_mwh       energía activada en el día (MW medios × horas); nula si el TSO no publica la activada
--   activated_isp_share parte de los intervalos con dato de activación en que se activó algo
-- Productos: aFRR, mFRR (total), mFRR_scheduled y mFRR_direct. No todos los TSO publican todos.
select
    area, product, direction, market_date,
    date_trunc('month', market_date)::date                                   as month,
    avg(offered_mw)                                                          as offered_mw_avg,
    max(offered_mw)                                                          as offered_mw_max,
    sum(activated_mw * hours)                                                as activated_mwh,
    max(activated_mw)                                                        as activated_mw_max,
    avg((activated_mw > 0)::int) filter (where activated_mw is not null)     as activated_isp_share,
    avg(unavailable_mw)                                                      as unavailable_mw_avg,
    sum(hours)                                                               as hours_covered
from {{ ref('stg_balancing_bids') }}
group by area, product, direction, market_date
