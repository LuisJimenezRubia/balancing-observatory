# Power BI report — design

Semantic model defined as code (TMDL, `serve/powerbi/`) on the Parquet/Delta tables exported by
`serve/export_gold.py`. Built in Power BI Desktop (Windows PC, reading the Mac's shared folder) first; Direct Lake on
Microsoft Fabric at the end, as in grid-lakehouse.

This document comes before gold on purpose: every visual below names the mart it reads, so the list of marts is the
gold backlog.

## Model

Star schema, single-direction many-to-one relationships.

| From (many) | Column | To (one) | Column |
|---|---|---|---|
| fct_imbalance, fct_cbmp_15min, fct_activated_energy, fct_forecast_error, mart_daily_* | market_date | dim_date | market_date |
| mart_monthly_* | month | dim_date | market_date (first day of month) |
| every fact and mart | area | dim_area | area |
| fct_cbmp_15min, fct_activated_energy, fct_contracted_reserves | product (aFRR, mFRR, FCR, RR) | dim_product | product |
| fct_forecast_error | technology (wind_onshore, wind_offshore, solar, load) | dim_technology | technology |

`dim_area` comes from the seed `areas.csv`: EIC, country, TSO, area type (control area / bidding zone / country),
imbalance-price area (the four German control areas share one price, `DE`), and the dates each TSO joined PICASSO
and MARI. Those dates drive the before/after comparisons; they are the most important attribute in the model.

Time: `interval_start_utc` is the true instant; `interval_start_local`, `hour_local` and `quarter_of_day` are
Brussels wall-clock values for hour-of-day visuals. Mark `dim_date` as the date table. Clock-change days have 92 or
100 quarter-hours; tests expect the right count.

## Gold tables the report needs

| Table | Grain | Source silver | Used on pages |
|---|---|---|---|
| `fct_imbalance` | area × 15 min: imbalance price (EUR/MWh), imbalance volume (MW, signed), imbalance cost | A85, A86 | 1, 2 |
| `fct_cbmp_15min` | area × direction × 15 min: VWAP, min, max, std, n_cycles, n_cycles_with_activation, from the 4 s CBMP | A84+A67 | 1, 3 |
| `fct_cbmp_spread_15min` | area × 15 min: CBMP VWAP minus the reference area's (var `cbmp_reference_zone`), convergence flag (± var `cbmp_convergence_eur_mwh`) | fct_cbmp_15min | 3 |
| `fct_activated_energy` | area × product × direction × 15 min: activated MWh (A24), of which exchanged via platform (B17 A51 aggregated), activation price (A84 A16/A68) | A24, B17, A84 | 4 |
| `fct_netting` | area × 15 min: IGCC netted import/export MWh | B17+A63 | 4 |
| `fct_contracted_reserves` | area × product × direction × day: contracted MW and price EUR/MW/h | A81 | 5 |
| `fct_forecast_error` | area × technology × 15/60 min: day-ahead forecast, actual, error, abs error, % error | A69, A74, A65 | 6 |
| `mart_monthly_imbalance_cost` | area × month: Σ\|imbalance\|×price / Σ\|imbalance\| (EUR/MWh), mean price, p95, share of ISPs above 500 EUR/MWh, months since PICASSO/MARI accession | fct_imbalance, dim_area | 1, 2 |
| `mart_hourly_imbalance_profile` | area × hour_local: mean price, mean abs imbalance | fct_imbalance | 2 |
| `mart_monthly_cbmp_convergence` | area × direction × month: share of ISPs converged with reference, mean spread, p95 spread, max spread, extreme-price share (vars) | fct_cbmp_spread_15min | 3 |
| `mart_daily_cbmp_divergence` | area × date: ISPs diverged, max spread, worst hour (for the "what happened that day" drill) | fct_cbmp_spread_15min | 3 |
| `mart_monthly_activation_mix` | area × product × month: activated MWh home vs exchanged, netted MWh, share exchanged, mean activation price | fct_activated_energy, fct_netting | 4 |
| `mart_monthly_capacity_vs_energy` | area × product × month: capacity cost (MW × price × h) vs energy cost (MWh × price) | fct_contracted_reserves, fct_activated_energy | 5 |
| `mart_monthly_forecast_quality` | area × technology × month: MAE (MW), bias (MW), nMAE (% of installed or mean), RMSE | fct_forecast_error | 6 |
| `mart_hourly_forecast_quality` | area × technology × hour_local: MAE, bias | fct_forecast_error | 6 |
| `mart_forecast_error_vs_imbalance` | area × month: correlation(error renewables + load, system imbalance), correlation(abs error, imbalance price), imbalance cost per MW of forecast error | fct_forecast_error, fct_imbalance | 6 |

Dimensions: `dim_date` (as grid-lakehouse), `dim_area` (seed), `dim_product` (seed), `dim_technology` (seed).

## Measures (DAX)

```dax
-- Imbalance
Imbalance cost (€/MWh) = DIVIDE ( SUMX ( fct_imbalance, ABS ( fct_imbalance[imbalance_mw] ) * 0.25 * fct_imbalance[imbalance_price_eur_mwh] ),
                                  SUMX ( fct_imbalance, ABS ( fct_imbalance[imbalance_mw] ) * 0.25 ) )
Avg imbalance price (€/MWh) = AVERAGE ( fct_imbalance[imbalance_price_eur_mwh] )
Extreme ISP share % = DIVIDE ( CALCULATE ( COUNTROWS ( fct_imbalance ), ABS ( fct_imbalance[imbalance_price_eur_mwh] ) > 500 ),
                               COUNTROWS ( fct_imbalance ) )
Mean abs imbalance (MW) = AVERAGEX ( fct_imbalance, ABS ( fct_imbalance[imbalance_mw] ) )

-- CBMP (PICASSO)
CBMP VWAP (€/MWh) = DIVIDE ( SUMX ( fct_cbmp_15min, fct_cbmp_15min[vwap_eur_mwh] * fct_cbmp_15min[activated_mwh] ),
                             SUM ( fct_cbmp_15min[activated_mwh] ) )
CBMP convergence % = DIVIDE ( CALCULATE ( COUNTROWS ( fct_cbmp_spread_15min ), fct_cbmp_spread_15min[is_converged] = TRUE () ),
                              COUNTROWS ( fct_cbmp_spread_15min ) )
CBMP mean spread (€/MWh) = AVERAGEX ( fct_cbmp_spread_15min, ABS ( fct_cbmp_spread_15min[spread_eur_mwh] ) )
CBMP p95 spread (€/MWh)  = PERCENTILEX.INC ( fct_cbmp_spread_15min, ABS ( fct_cbmp_spread_15min[spread_eur_mwh] ), 0.95 )
CBMP extreme share % = DIVIDE ( CALCULATE ( COUNTROWS ( fct_cbmp_15min ), fct_cbmp_15min[max_eur_mwh] > 1000 || fct_cbmp_15min[min_eur_mwh] < -500 ),
                                COUNTROWS ( fct_cbmp_15min ) )

-- Activation and exchange
Activated energy (GWh) = DIVIDE ( SUM ( fct_activated_energy[activated_mwh] ), 1000 )
Exchanged share % = DIVIDE ( SUM ( fct_activated_energy[exchanged_mwh] ), SUM ( fct_activated_energy[activated_mwh] ) )
Netted energy (GWh) = DIVIDE ( SUM ( fct_netting[netted_mwh] ), 1000 )
Avg activation price (€/MWh) = DIVIDE ( SUMX ( fct_activated_energy, fct_activated_energy[activated_mwh] * fct_activated_energy[price_eur_mwh] ),
                                        SUM ( fct_activated_energy[activated_mwh] ) )

-- Capacity vs energy
Capacity cost (M€) = DIVIDE ( SUM ( mart_monthly_capacity_vs_energy[capacity_cost_eur] ), 1e6 )
Energy cost (M€)   = DIVIDE ( SUM ( mart_monthly_capacity_vs_energy[energy_cost_eur] ), 1e6 )
Capacity share of cost % = DIVIDE ( [Capacity cost (M€)], [Capacity cost (M€)] + [Energy cost (M€)] )

-- Forecast quality
Forecast MAE (MW)  = AVERAGE ( fct_forecast_error[abs_error_mw] )
Forecast bias (MW) = AVERAGE ( fct_forecast_error[error_mw] )
Forecast nMAE %    = DIVIDE ( [Forecast MAE (MW)], AVERAGE ( fct_forecast_error[actual_mw] ) )
Error–imbalance correlation = AVERAGE ( mart_forecast_error_vs_imbalance[corr_error_imbalance] )
```

## Pages

1. **Overview** — cards: areas covered, days loaded, automated tests passed, imbalance cost (€/MWh) for the
   selected area vs the average of all areas, CBMP convergence %, exchanged share %. Line: monthly imbalance cost per area
   with vertical markers at each area's PICASSO/MARI accession date (from `dim_area`). The headline chart of the project.
2. **Imbalance prices** — small multiples of monthly mean and p95 imbalance price per area; heatmap area × hour of day
   of mean imbalance price (`mart_hourly_imbalance_profile`); table of the 20 most expensive ISPs with area, date,
   price, system imbalance. Caveat: Germany's reBAP is one price for four control areas.
3. **PICASSO convergence** — convergence % by area and month (reference area selectable, default 50Hertz); 100 %
   stacked bar converged / small spread / large spread per area; p95 spread by month; drill to
   `mart_daily_cbmp_divergence` for the worst days, then to the 15-min series of that day with CBMP of each area
   overlaid. Note on resolution: 4 s source aggregated to 15 min VWAP; max/min kept.
4. **Activation and exchange** — monthly stacked columns per area: activated at home vs exchanged via platform, by
   product (aFRR / mFRR); line of IGCC netted energy; exchanged share % before and after accession per area.
5. **Capacity vs energy** — clustered columns capacity cost vs energy cost per area and month; capacity share of
   cost by product; contracted MW by product and area over time.
6. **Forecast quality** — MAE and bias by TSO and technology (wind onshore / offshore, solar, load), monthly and by
   hour of day; scatter forecast error vs system imbalance per area (sampled hourly); bar of imbalance cost per MW of
   forecast error per area, the bridge between the two halves of the project.
7. **Data quality** — coverage matrix dataset × area × month (ok / partial / missing); publication lags; days
   re-downloaded in the revision window and what changed; list of known source quirks (A03 curves, ZIP payloads,
   process-type migration per TSO, Amprion not publishing A84, German price under the country EIC).

Every page carries the source line (ENTSO-E Transparency Platform, data as published by each TSO) and, where
needed, the caveat that platform accession dates and publication practices differ by TSO, so before/after
comparisons are per area, never pooled.

## Open design points

- Reference area for convergence: 50Hertz by default (longest PICASSO history), but a "European average" option
  (volume-weighted VWAP across connected areas) would be more neutral; decide after seeing the data.
- Whether to show the 4 s CBMP anywhere: probably one illustrative day on page 3 as a zoom-in, exported separately.
- Forecast error normalisation: by mean actual (nMAE) in v1; installed capacity (A68 document type) in phase 2.
