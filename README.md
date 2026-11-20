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

1. How volatile is the imbalance price in each area, how often does it hit extreme values, and how has that
   changed since the TSO joined PICASSO and MARI?
2. Do aFRR prices converge across areas? Share of 4-second optimisation cycles in which an area has exactly the
   same cross-border marginal price as Germany, and how far apart prices are when it does not.
3. How much balancing energy is offered and how much is activated, at what price, and how much activation is
   avoided by netting imbalances across areas (IGCC)?
4. How good are the day-ahead wind, solar and load forecasts in each zone, and how much of the system imbalance
   moves with the forecast error?
5. How much reserve capacity does each TSO contract, by product and direction, and how does it evolve?

## Status

Bronze, silver and gold are built and tested (124 dbt tests). The Power BI report is the next step.

| Layer | What is there |
|---|---|
| Bronze | Raw API responses for 16 datasets, January 2023 onwards for the light ones, one year of 4-second PICASSO data, plus ECB reference rates. Every request is logged. |
| Silver | 13 Delta tables in UTC: imbalance prices and volumes, PICASSO CBMP aggregated to 15 minutes and its cycle-level convergence, IGCC netting, contracted reserves, activation prices, offered and activated balancing energy, wind / solar / load forecasts and actuals. |
| Gold | dbt models on DuckDB: facts and monthly marts for imbalance, CBMP convergence, reserves, balancing energy, netting and forecast quality. |

First findings and the points where the source data forced a design change are in `docs/report-design.md`.
Domain notes are in `docs/conceptos-balance.md` (Spanish for now; English version to follow).

## How to run

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
echo "ENTSOE_TOKEN=your-token" > .env                      # free token from the ENTSO-E Transparency Platform
python3 ingest/bronze.py --start 2026-10-14 --end 2026-10-14   # one day, all datasets
python3 ingest/fx.py                                        # ECB reference rates
python3 transform/silver.py                                 # bronze -> silver (Delta)
mkdir -p data/gold && cd gold && dbt build --profiles-dir . # silver -> gold, with tests
```

## Data sources

Data © [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/), used under its terms of use.
Validation references: [Elia Open Data](https://opendata.elia.be/) and ACER balancing monitoring reports.
This repository contains code only, no data.
