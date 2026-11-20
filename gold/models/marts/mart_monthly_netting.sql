-- Neteo IGCC mensual por área de control.
select area, month,
       sum(netted_import_mwh) as netted_import_mwh,
       sum(netted_export_mwh) as netted_export_mwh,
       sum(netted_mwh)        as netted_mwh,
       count(*)               as days
from {{ ref('fct_netting') }}
group by area, month
