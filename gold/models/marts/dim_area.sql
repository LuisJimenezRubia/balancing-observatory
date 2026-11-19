-- Áreas del observatorio, con el TSO, el área de precio de desvío a la que pertenecen y las fechas de adhesión
-- a PICASSO y MARI, que son las que marcan las comparaciones antes/después.
select
    area,
    eic_code,
    area_name,
    country_code,
    tso,
    area_type,
    imbalance_price_area,
    area = imbalance_price_area              as is_imbalance_price_area,
    cast(picasso_since as date)              as picasso_since,
    cast(mari_since as date)                 as mari_since,
    accession_note
from {{ ref('areas') }}
