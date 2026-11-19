-- Convergencia mensual del CBMP de cada área con la referencia, por sentido.
-- Las cuotas van ponderadas por ciclos (no por cuartos de hora), para que un cuarto de hora con tres ciclos
-- comparables no pese igual que uno con doscientos.
select
    area,
    reference_area,
    direction,
    month,
    count(*)                                                                    as isp_count,
    sum(cycles_both_priced)                                                     as cycles_both_priced,
    sum(cycles_both_priced) / sum(cycles_total)::double                         as comparable_cycle_share,
    sum(cycles_equal_price) / nullif(sum(cycles_both_priced), 0)::double        as convergence_share,
    avg(is_fully_converged::int) filter (where cycles_both_priced > 0)          as fully_converged_isp_share,
    sum(mean_abs_spread_eur_mwh * cycles_both_priced)
        / nullif(sum(cycles_both_priced), 0)                                    as mean_abs_spread_eur_mwh,
    quantile_cont(mean_abs_spread_eur_mwh, 0.95)                                as p95_isp_abs_spread_eur_mwh,
    max(max_abs_spread_eur_mwh)                                                 as max_abs_spread_eur_mwh,
    sum(cycles_only_area) / sum(cycles_total)::double                           as only_area_cycle_share,
    sum(cycles_only_reference) / sum(cycles_total)::double                      as only_reference_cycle_share
from {{ ref('fct_cbmp_convergence_15min') }}
group by area, reference_area, direction, month
