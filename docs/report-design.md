# Power BI report — design

Semantic model defined as code (TMDL, `serve/powerbi/`) on the Parquet tables exported from gold by
`serve/export_gold.py`. Built in Power BI Desktop first; Direct Lake on Microsoft Fabric at the end, as in
grid-lakehouse.

Every visual below names the gold table it reads. All of them exist and are tested (`gold/models/marts/`).
This document was rewritten on 2026-11-20 after the data was loaded: the section "What the data changed" lists
the points where the first design did not survive contact with the source.

## Model

Star schema, single-direction many-to-one relationships.

| From (many) | Column | To (one) | Column |
|---|---|---|---|
| every fact with `market_date` | market_date | dim_date | market_date |
| every mart with `month` | month | dim_date | market_date (first day of the month) |
| every fact and mart | area | dim_area | area |
| fct_forecast_error, mart_*_forecast_quality | technology | dim_technology | technology |

`dim_area` (seed `areas.csv`): EIC, country, TSO, area type (control area / bidding zone / country), the
imbalance-price area it belongs to, and the dates the TSO joined PICASSO and MARI, with the source of each date in
`accession_note`. Those dates drive the before/after comparisons.

Two area grains coexist and the report must not mix them:

- **Imbalance-price areas** (AT, BE, CZ, DE, DK1, DK2, FR, NL): `fct_imbalance`, `fct_cbmp_15min`,
  `fct_cbmp_convergence_15min`. Germany is one area: one imbalance price (reBAP), and the German control areas
  publish an identical CBMP.
- **Control areas** (the same plus DE_50HZ, DE_AMPRION, DE_TENNET, DE_TRANSNETBW instead of DE): reserves, offered
  and activated energy, activation prices, netting.
- **Bidding zones** for the forecast benchmark (DE_LU instead of the German areas).

Time: `interval_start_utc` is the true instant; `interval_start_local` and `hour_local` are Brussels wall-clock
values for hour-of-day visuals. Mark `dim_date` as the date table. `dim_date.quarters_in_day` is 92, 96 or 100.

## Gold tables

| Table | Grain | Used on pages |
|---|---|---|
| `fct_imbalance` | price area × 15 min: imbalance price (EUR/MWh, surplus and deficit sides), signed system imbalance (MWh, positive = long) | 1, 2 |
| `mart_monthly_imbalance_cost` | price area × month: mean, median, p05, p95, standard deviation, share of ISPs beyond ±500 EUR/MWh, imbalance-weighted price, months since PICASSO / MARI accession | 1, 2 |
| `fct_cbmp_15min` | price area × direction × 15 min: mean, min, max, std of the CBMP over the 4-second cycles that carry a price; share of cycles that do | 3 |
| `fct_cbmp_convergence_15min` | price area × direction × 15 min: cycles where area and reference both have a price, cycles where it is identical, spread | 3 |
| `mart_monthly_cbmp_convergence` | price area × direction × month: cycle-weighted convergence share, fully converged ISP share, mean and p95 spread | 1, 3 |
| `fct_contracted_reserves` | control area × product × direction × day: contracted MW, procurement price as published | 5 |
| `fct_balancing_energy` | control area × product × direction × day: offered MW, activated MWh | 4 |
| `fct_activation_price` | control area × product × process × direction × price category × day: price of activated energy (EUR/MWh) | 4 |
| `fct_netting` / `mart_monthly_netting` | control area × day / month: IGCC netted energy, import and export | 4 |
| `mart_monthly_balancing_overview` | control area × product × direction × month: contracted MW, offered MW, activated MWh, activation price | 4, 5 |
| `fct_forecast_error` | zone × technology × hour: day-ahead forecast, actual, error, `is_comparable` | 6 |
| `mart_monthly_forecast_quality`, `mart_hourly_forecast_quality` | zone × technology × month / hour of day: MAE, bias, RMSE, normalised MAE | 6 |
| `mart_forecast_error_vs_imbalance` | price area × month: correlation between net forecast error and system imbalance | 6 |

Dimensions: `dim_date`, `dim_area`, `dim_technology`.

## Measures (DAX)

```dax
-- Imbalance (fct_imbalance; system_imbalance_mwh is energy per quarter-hour, already in MWh)
Avg imbalance price (€/MWh) = AVERAGE ( fct_imbalance[imbalance_price_eur_mwh] )
Imbalance price std (€/MWh) = STDEV.S ( fct_imbalance[imbalance_price_eur_mwh] )
Extreme ISP share % = DIVIDE ( CALCULATE ( COUNTROWS ( fct_imbalance ), ABS ( fct_imbalance[imbalance_price_eur_mwh] ) > 500 ),
                               COUNTROWS ( fct_imbalance ) )
Imbalance-weighted price (€/MWh) = DIVIDE ( SUMX ( fct_imbalance, ABS ( fct_imbalance[system_imbalance_mwh] ) * fct_imbalance[imbalance_price_eur_mwh] ),
                                            SUMX ( fct_imbalance, ABS ( fct_imbalance[system_imbalance_mwh] ) ) )
Mean abs imbalance (MWh per ISP) = AVERAGEX ( fct_imbalance, ABS ( fct_imbalance[system_imbalance_mwh] ) )
System short share % = DIVIDE ( CALCULATE ( COUNTROWS ( fct_imbalance ), fct_imbalance[system_imbalance_mwh] < 0 ),
                                CALCULATE ( COUNTROWS ( fct_imbalance ), NOT ISBLANK ( fct_imbalance[system_imbalance_mwh] ) ) )

-- PICASSO (convergence is counted per 4-second cycle, so the ratio must be taken on the cycle sums)
CBMP convergence % = DIVIDE ( SUM ( fct_cbmp_convergence_15min[cycles_equal_price] ),
                              SUM ( fct_cbmp_convergence_15min[cycles_both_priced] ) )
Comparable cycles % = DIVIDE ( SUM ( fct_cbmp_convergence_15min[cycles_both_priced] ),
                               SUM ( fct_cbmp_convergence_15min[cycles_total] ) )
CBMP mean abs spread (€/MWh) = DIVIDE ( SUMX ( fct_cbmp_convergence_15min, fct_cbmp_convergence_15min[mean_abs_spread_eur_mwh] * fct_cbmp_convergence_15min[cycles_both_priced] ),
                                        SUM ( fct_cbmp_convergence_15min[cycles_both_priced] ) )
Fully converged ISP % = DIVIDE ( CALCULATE ( COUNTROWS ( fct_cbmp_convergence_15min ), fct_cbmp_convergence_15min[is_fully_converged] = TRUE () ),
                                 CALCULATE ( COUNTROWS ( fct_cbmp_convergence_15min ), fct_cbmp_convergence_15min[cycles_both_priced] > 0 ) )
CBMP mean (€/MWh) = DIVIDE ( SUMX ( fct_cbmp_15min, fct_cbmp_15min[cbmp_mean_eur_mwh] * fct_cbmp_15min[cycles_with_price] ),
                             SUM ( fct_cbmp_15min[cycles_with_price] ) )

-- Balancing energy, reserves, netting
Offered (MW) = AVERAGE ( fct_balancing_energy[offered_mw_avg] )
Activated energy (GWh) = DIVIDE ( SUM ( fct_balancing_energy[activated_mwh] ), 1000 )
Contracted reserve (MW) = AVERAGE ( fct_contracted_reserves[contracted_mw] )
Netted energy (GWh) = DIVIDE ( SUM ( fct_netting[netted_mwh] ), 1000 )

-- Forecast quality (benchmark visuals carry the filter fct_forecast_error[is_comparable] = TRUE)
Forecast MAE (MW)  = AVERAGE ( fct_forecast_error[abs_error_mw] )
Forecast bias (MW) = AVERAGE ( fct_forecast_error[error_mw] )
Forecast nMAE %    = DIVIDE ( [Forecast MAE (MW)], AVERAGE ( fct_forecast_error[actual_mw] ) )
```

## Pages

1. **Overview** — cards: areas covered, days loaded, automated tests passed. Line of monthly imbalance-price
   standard deviation per area with a marker at each area's PICASSO accession; small table "12 months before /
   after accession" (standard deviation, extreme ISP share). Second line: monthly CBMP convergence with Germany.
   The headline is volatility and tails, not the price level: the mean follows the wholesale market.
2. **Imbalance prices** — small multiples of monthly mean and p95 per area; heatmap area × hour of day; table of
   the most extreme ISPs (Belgium at the −15 000 EUR/MWh floor on 2026-04-06). Caveats on the page: Germany is one
   price for four control areas; France publishes two prices (surplus ≠ deficit) almost always and the Netherlands
   increasingly; system imbalance volume is missing for Denmark after July 2025 and for the Netherlands before 2025.
3. **PICASSO convergence** — convergence % with Germany by area and month, one line per direction; comparable
   cycles % next to it, so the reader sees how much of the time the comparison exists at all; mean and p95 spread;
   drill to the quarter-hours of a day. Reference area fixed to Germany (50Hertz).
4. **Activation, offers and netting** — mFRR offered MW against activated energy (scheduled and direct) by area and
   month; activation price by product and direction; IGCC netted energy by area. aFRR appears with price and
   offered volume only.
5. **Contracted reserves** — contracted MW by product (FCR, aFRR, mFRR, RR), direction and area over time;
   procurement price as an index per area (first month = 100), never as a cross-area comparison.
6. **Forecast quality** — normalised MAE and bias by zone and technology (wind onshore / offshore, solar, load),
   monthly and by hour of day, filtered to comparable series; correlation of the net forecast error with the
   system imbalance per area.
7. **Data quality** — coverage by dataset, area and month; the series flagged as not comparable and why
   (`forecast_comparability` seed); source gaps (France imbalance data with 139 incomplete days, IGCC netting on
   the 2023 clock-change days, Amprion publishing almost no balancing data); publication quirks (A03 curves,
   mixed resolutions, CZK prices, 100-document limit per request, clock-change days truncated in monthly requests).

Every page carries the source line (ENTSO-E Transparency Platform, data as published by each TSO; ECB reference
rates for currency conversion).

## What the data changed

- **No volume-weighted CBMP.** The CBMP dataset carries a price per 4-second cycle and no volume, so a VWAP cannot
  be computed. The quarter-hour figure is the plain mean over the cycles that have a price.
- **Convergence is measured per cycle, not on quarter-hour means.** Comparing 15-minute means gives about 20 %
  convergence; counting cycles with an identical price gives 51–88 % depending on area and direction. Two areas with
  the same price in every cycle still have different means when one of them had activation in more cycles.
- **Activated aFRR volume is not published** in the aggregated-bids dataset (only offered volume, for Belgium and
  Austria). Activated mFRR is, as scheduled and direct activation. The page "activated at home vs exchanged" became
  "offered vs activated mFRR".
- **Capacity cost in euros is not computed.** The procurement price comes in EUR/MW without saying whether it is
  per hour or per contracted period, and that can differ by TSO.
- **System imbalance comes in MWh per settlement period**, not MW; its sign is not documented and was derived from
  the data (the "down" series coincides with high imbalance prices, i.e. the system is short).
- **Czech imbalance prices are published in CZK** and converted with the ECB daily reference rate.
- **Forecast benchmark at hourly grain**, because zones publish at 15 or 60 minutes; the Netherlands and DK1
  offshore wind are excluded because forecast and actual do not cover the same perimeter.
- **Imbalance cost "before / after accession" needs history**, so the light datasets were loaded from January 2023
  instead of one year.

## Open points

- Exact PICASSO accession day for Belgium and the Netherlands, and the MARI accession date for France (2026):
  validate against the CBMP data once the series is loaded back to 2022.
- Why French convergence with Germany steps up from about 45 % to about 66 % in July 2026.
- Cross-border aFRR exchange (B17 + A51, 4-second data): loaded in bronze, not yet in silver.
- Whether to show one illustrative day of 4-second CBMP on page 3.
