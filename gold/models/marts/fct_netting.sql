-- Energía neteada en IGCC por área de control y día de mercado: lo que el área evitó activar porque su desvío
-- se compensó con el de otras áreas. import / export son los dos sentidos del intercambio con la plataforma.
select
    area, market_date,
    date_trunc('month', market_date)::date                              as month,
    coalesce(sum(netted_mwh) filter (where direction = 'import'), 0)    as netted_import_mwh,
    coalesce(sum(netted_mwh) filter (where direction = 'export'), 0)    as netted_export_mwh,
    sum(netted_mwh)                                                     as netted_mwh,
    count(distinct interval_start_utc)                                  as intervals
from {{ ref('stg_netting') }}
group by area, market_date
