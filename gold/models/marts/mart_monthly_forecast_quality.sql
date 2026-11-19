-- Calidad mensual de la previsión day-ahead por zona y tecnología.
--   mae_mw / bias_mw / rmse_mw   en MW (bias > 0: se previó de menos)
--   nmae                         MAE dividido por la media real del mes: permite comparar zonas de distinto tamaño
--   hour_coverage                horas con previsión y real completos sobre las horas del mes con algún dato
select
    f.area,
    f.technology,
    f.month,
    count(*)                                        as hours,
    avg(f.actual_mw)                                as mean_actual_mw,
    avg(f.abs_error_mw)                             as mae_mw,
    avg(f.error_mw)                                 as bias_mw,
    sqrt(avg(f.error_mw * f.error_mw))              as rmse_mw,
    avg(f.abs_error_mw) / nullif(avg(f.actual_mw), 0) as nmae,
    avg(f.error_mw) / nullif(avg(f.actual_mw), 0)   as normalised_bias,
    count(*) / any_value(t.hours_with_data)::double as hour_coverage
from {{ ref('fct_forecast_error') }} f
join (
    select area, technology,
           date_trunc('month', {{ hora_bruselas('hour_start_utc') }})::date as month,
           count(*) as hours_with_data
    from {{ ref('stg_forecast_actual_hourly') }}
    group by 1, 2, 3
) t on t.area = f.area and t.technology = f.technology and t.month = f.month
group by f.area, f.technology, f.month
