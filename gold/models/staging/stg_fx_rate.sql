-- Tipo de cambio de referencia del BCE por moneda y día hábil (unidades de moneda por euro).
select currency, rate_date, units_per_eur
from {{ silver('ecb_fx_rate') }}
