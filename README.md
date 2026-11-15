# European Balancing Observatory

A medallion lakehouse on **public European balancing-market data** from the ENTSO-E Transparency Platform:
imbalance prices and volumes, PICASSO cross-border marginal prices (4-second resolution), activated balancing
energy, contracted reserves, imbalance netting and cross-border aFRR exchanges, plus day-ahead wind, solar and
load forecasts against actuals. Eleven balancing areas: BE, NL, FR, AT, CZ, DK1, DK2 and the four German control areas (Germany has a single imbalance price).

Built locally at zero cost with Python, Delta Lake, DuckDB and dbt; designed to run on Microsoft Fabric.
Companion to [grid-lakehouse](https://github.com/LuisJimenezRubia/grid-lakehouse), which covers day-ahead prices,
cross-border flows and flow-based market coupling.

> Personal portfolio project by Magic Byte SRL. Public data only.

## Questions it answers

1. What does imbalance cost in each area (EUR per MWh of imbalance, by month and hour of day), and how has it
   changed since the TSOs joined PICASSO and MARI?
2. Do aFRR prices converge across areas? Share of quarter-hours with the same CBMP, mean and tail spread, and
   when and why they diverge.
3. How much balancing energy is activated at home versus exchanged across borders?
4. How good are each TSO's wind, solar and load forecasts, and how much of the system imbalance and the
   imbalance price do forecast errors explain?
5. What does reserving capacity cost compared with activating energy, by area and product?

## Status

Data-source catalogue closed (354 API probes, see `docs/catalogo_balancing.txt`). Domain notes and scope in
`docs/conceptos-balance.md` (Spanish for now; English version to follow). Ingestion in progress.

## Data sources

Data © [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/), used under its terms of use.
Validation references: [Elia Open Data](https://opendata.elia.be/) and ACER balancing monitoring reports.
This repository contains code only, no data.
