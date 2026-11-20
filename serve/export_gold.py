#!/usr/bin/env python3
"""Capa de servicio: exporta las tablas de gold que usa el informe de Power BI.

Salida, sobrescrita en cada ejecución:
  - data/serving/<tabla>/  tablas Delta y _manifest.json. Se suben tal cual a la sección Tables/ de un
    Lakehouse de Microsoft Fabric, donde un modelo semántico en modo Direct Lake las lee sin copiarlas.
  - data/serving_parquet/<tabla>.parquet  un fichero por tabla, para Power BI Desktop (conector Parquet,
    modo Import) desde el PC con Windows, a través de la carpeta compartida del Mac. Power BI Desktop no
    lee bien tablas Delta de una carpeta local; Parquet sí.

Ajustes para Power BI:
  - Las horas locales (columnas *_local, hora de pared de Bruselas) van como timestamp "UTC" en Delta, para
    evitar el tipo timestamp_ntz que no todos los lectores aceptan, y sin zona en Parquet, para que Power Query
    las lea como datetime y las muestre tal cual.
  - Las tablas a 15 min ganan quarter_of_day (0-95) para los gráficos por cuarto de hora del día.
  - Los enteros de 128 bits de DuckDB (sumas) pasan a int64 y los decimales a double.

Uso (desde la carpeta del proyecto, con el entorno virtual activado y después de dbt build):
  python3 serve/export_gold.py
"""
import argparse
import datetime as dt
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake

RAIZ = Path(__file__).resolve().parent.parent
GOLD = RAIZ / "data" / "gold" / "balancing.duckdb"

# tabla publicada → origen en balancing.duckdb. Dimensiones primero, luego hechos y marts.
TABLAS = {
    "dim_date": "gold.dim_date",
    "dim_area": "gold.dim_area",
    "dim_technology": "gold.dim_technology",
    "dq_forecast_comparability": "reference.forecast_comparability",
    "fct_imbalance": "gold.fct_imbalance",
    "fct_cbmp_15min": "gold.fct_cbmp_15min",
    "fct_cbmp_convergence_15min": "gold.fct_cbmp_convergence_15min",
    "fct_contracted_reserves": "gold.fct_contracted_reserves",
    "fct_balancing_energy": "gold.fct_balancing_energy",
    "fct_activation_price": "gold.fct_activation_price",
    "fct_netting": "gold.fct_netting",
    "fct_forecast_error": "gold.fct_forecast_error",
    "mart_monthly_imbalance_cost": "gold.mart_monthly_imbalance_cost",
    "mart_monthly_cbmp_convergence": "gold.mart_monthly_cbmp_convergence",
    "mart_monthly_balancing_overview": "gold.mart_monthly_balancing_overview",
    "mart_monthly_netting": "gold.mart_monthly_netting",
    "mart_monthly_forecast_quality": "gold.mart_monthly_forecast_quality",
    "mart_hourly_forecast_quality": "gold.mart_hourly_forecast_quality",
    "mart_forecast_error_vs_imbalance": "gold.mart_forecast_error_vs_imbalance",
}


def consulta(con, origen):
    columnas = [c[0] for c in con.sql(f"describe {origen}").fetchall()]
    if "interval_start_utc" not in columnas or "quarter_of_day" in columnas:
        return f"select * from {origen}"
    local = "timezone('Europe/Brussels', interval_start_utc)"   # hora de pared de Bruselas, sin zona
    return f"select *, (hour({local}) * 4 + minute({local}) // 15)::int as quarter_of_day from {origen}"


def normalizar(tabla):
    """Tipos que Delta y Power BI leen sin sorpresas."""
    campos = []
    for f in tabla.schema:
        t = f.type
        if pa.types.is_decimal(t):
            t = pa.float64()
        elif pa.types.is_integer(t) and t.bit_width > 64:
            t = pa.int64()
        elif pa.types.is_timestamp(t):      # con zona → UTC; hora de pared sin zona → misma hora marcada como UTC
            t = pa.timestamp("us", tz="UTC")
        campos.append(pa.field(f.name, t))
    return tabla.cast(pa.schema(campos))


def para_parquet(tabla):
    """Para Power BI Desktop: las horas locales, sin zona, para que Power Query las lea como datetime."""
    for i, f in enumerate(tabla.schema):
        if f.name.endswith("_local") and pa.types.is_timestamp(f.type):
            tabla = tabla.set_column(i, pa.field(f.name, pa.timestamp("us")), tabla.column(i).cast(pa.timestamp("us")))
    return tabla


def main():
    p = argparse.ArgumentParser(description="Gold → serving tables for Power BI (Delta + Parquet)")
    p.add_argument("--out", type=Path, default=RAIZ / "data", help="folder for serving/ and serving_parquet/ (default: ./data)")
    a = p.parse_args()
    salida, salida_parquet = a.out / "serving", a.out / "serving_parquet"
    salida.mkdir(parents=True, exist_ok=True)
    salida_parquet.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(GOLD), read_only=True)
    con.sql("set TimeZone = 'UTC'")
    manifiesto = {"exported_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "tables": {}}
    for nombre, origen in TABLAS.items():
        rel = con.sql(consulta(con, origen))
        tabla = normalizar(rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.fetch_arrow_table())
        write_deltalake(str(salida / nombre), tabla, mode="overwrite", schema_mode="overwrite")
        pq.write_table(para_parquet(tabla), salida_parquet / f"{nombre}.parquet", compression="snappy")
        manifiesto["tables"][nombre] = {"source": origen, "rows": tabla.num_rows, "columns": tabla.num_columns}
        print(f"  {nombre:34} {tabla.num_rows:>10,} filas  {tabla.num_columns:>3} columnas")
    (salida / "_manifest.json").write_text(json.dumps(manifiesto, indent=2), encoding="utf-8")
    print(f"\nTerminado: {len(TABLAS)} tablas Delta en {salida}/ y {len(TABLAS)} ficheros Parquet en {salida_parquet}/")


if __name__ == "__main__":
    main()
