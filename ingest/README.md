# Bronze ingestion

`bronze.py` downloads balancing and forecast data from the ENTSO-E Transparency Platform and stores each raw
response untouched under `data/bronze/entsoe/<dataset>/area=<AREA>/<dia=YYYY-MM-DD | mes=YYYY-MM>/`, logging every
request (status, points, bytes, sha256, duration) in `data/bronze/_registro_cargas.jsonl`.

Datasets, areas and API parameters come from the catalogue explored on 14-15/11/2026
(`docs/catalogo_balancing.txt`, `docs/conceptos-balance.md` section 8). Standard library only, plus `truststore`
for corporate proxies or antivirus that inspect https.

```bash
python3 ingest/bronze.py --start 2026-10-14 --end 2026-10-14                  # one test day, all datasets
python3 ingest/bronze.py --start 2025-10-01 --end 2026-10-31 --chunk month    # backfill, one month per request
python3 ingest/bronze.py --daily                                              # daily run: last 7 days
```

A failed request never stops the run: it is logged as an error and retried on the next run.
