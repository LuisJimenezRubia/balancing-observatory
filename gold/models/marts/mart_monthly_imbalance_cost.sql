-- Desvío por área y mes: nivel y cola del precio, precio ponderado por el desvío del sistema, y posición del
-- mes respecto a la adhesión del TSO a PICASSO y MARI. Es la tabla del gráfico principal del informe.
select
    f.area,
    f.month,
    count(*)                                                               as isp_count,
    avg(f.imbalance_price_eur_mwh)                                         as mean_price_eur_mwh,
    median(f.imbalance_price_eur_mwh)                                      as median_price_eur_mwh,
    avg(abs(f.imbalance_price_eur_mwh))                                    as mean_abs_price_eur_mwh,
    quantile_cont(f.imbalance_price_eur_mwh, 0.05)                         as p05_price_eur_mwh,
    quantile_cont(f.imbalance_price_eur_mwh, 0.95)                         as p95_price_eur_mwh,
    stddev_samp(f.imbalance_price_eur_mwh)                                 as price_std_eur_mwh,
    avg((abs(f.imbalance_price_eur_mwh) > 500)::int)                       as extreme_isp_share,
    avg(f.is_dual_price::int)                                              as dual_price_share,
    -- precio ponderado por el desvío absoluto del sistema: lo que cuesta de media un MWh de desvío
    sum(abs(f.system_imbalance_mwh) * f.imbalance_price_eur_mwh)
        / nullif(sum(abs(f.system_imbalance_mwh)), 0)                      as imbalance_weighted_price_eur_mwh,
    avg(abs(f.system_imbalance_mwh))                                       as mean_abs_imbalance_mwh,
    avg((f.system_imbalance_mwh < 0)::int)                                 as system_short_share,
    count(f.system_imbalance_mwh) / count(*)::double                       as volume_coverage,
    any_value(a.picasso_since)                                             as picasso_since,
    any_value(a.mari_since)                                                as mari_since,
    datediff('month', date_trunc('month', any_value(a.picasso_since)), f.month) as months_since_picasso,
    datediff('month', date_trunc('month', any_value(a.mari_since)), f.month)    as months_since_mari,
    f.month >= date_trunc('month', any_value(a.picasso_since))            as is_after_picasso
from {{ ref('fct_imbalance') }} f
join {{ ref('dim_area') }} a on a.area = f.area
group by f.area, f.month
