-- Energía neteada en IGCC por área de control, sentido (import / export respecto a la plataforma) e intervalo.
select area, direction, interval_start_utc, market_date, resolution, netted_mwh
from {{ silver('entsoe_netting') }}
