#!/usr/bin/env python3
"""Capa silver del observatorio: respuestas crudas de bronze → tablas Delta limpias, en UTC.

Tablas (quedan por añadir reservas, precios de activación, ofertas agregadas y CBMP, con el mismo patrón):
  entsoe_imbalance_price    precio de desvío por área, lado (excedente / déficit) e intervalo, en la moneda
                            que publica el TSO (EUR; Chequia en CZK): la conversión a EUR se hace en gold (A85)
  entsoe_imbalance_volume   volumen de desvío del sistema por área, sentido e intervalo            (A86)
  entsoe_netting            energía neteada en IGCC por área, sentido (import / export) e intervalo (B17 + A63)
  entsoe_wind_solar_forecast  previsión day-ahead eólica (on/offshore) y solar por zona            (A69 + A01)
  entsoe_wind_solar_actual    generación real eólica y solar por zona                              (A75)
  entsoe_load_forecast        previsión day-ahead de carga por zona                                (A65 + A01)
  entsoe_load_actual          carga real por zona                                                  (A65 + A16)

Reglas (comprobadas con los datos, ver docs/conceptos-balance.md):
  - Todo en UTC; cada fila lleva su día de mercado (hora de Bruselas) y el año, que es la partición.
  - curveType A03: ENTSO-E omite las posiciones cuyo valor se repite. Se rellenan con el último valor del mismo
    Period, nunca con 0 y nunca cruzando de un Period a otro.
  - Se conserva la resolución original (15, 30 o 60 min según área y año); gold la lleva a 15 min.
  - Un mismo intervalo puede estar en varios ficheros de bronze (mes, quincena partida, día suelto, día vuelto a
    descargar en la ventana de revisión): gana el descargado más tarde, que es la versión ya revisada por el TSO.
  - Carga idempotente: MERGE por clave natural; repetir un rango no duplica nada.

Uso (desde la carpeta del proyecto, con el entorno virtual activado):
  python3 transform/silver.py                                    todo lo que haya en bronze
  python3 transform/silver.py --start 2026-10-01 --end 2026-10-31
  python3 transform/silver.py --only imbalance_price
"""
import argparse
import calendar
import datetime as dt
import gzip
import io
import sys
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

RAIZ = Path(__file__).resolve().parent.parent
BRUSELAS = ZoneInfo("Europe/Brussels")
UTC = dt.timezone.utc
CUARTO = dt.timedelta(minutes=15)
TS = pa.timestamp("us", tz="UTC")
RESOLUCIONES = {"PT4S": dt.timedelta(seconds=4), "PT1M": dt.timedelta(minutes=1), "PT15M": CUARTO,
                "PT30M": dt.timedelta(minutes=30), "PT60M": dt.timedelta(hours=1)}
LADO_PRECIO = {"A04": "surplus", "A05": "deficit", "A06": "average"}   # imbalance_Price.category
SENTIDO = {"A01": "up", "A02": "down"}                                 # flowDirection.direction


# ---------------------------------------------------------------- bronze: ficheros y ventanas

def ventana(carpeta):
    """Días que cubre una carpeta de bronze: dia=AAAA-MM-DD, mes=AAAA-MM[_dd-dd] o rango=AAAA-MM-DD_AAAA-MM-DD."""
    tipo, valor = carpeta.split("=", 1)
    if tipo == "dia":
        d = dt.date.fromisoformat(valor)
        return d, d
    if tipo == "rango":
        a, b = valor.split("_")
        return dt.date.fromisoformat(a), dt.date.fromisoformat(b)
    anio, mes = map(int, valor[:7].split("-"))
    d1, d2 = (map(int, valor[8:].split("-")) if "_" in valor else (1, calendar.monthrange(anio, mes)[1]))
    return dt.date(anio, mes, d1), dt.date(anio, mes, d2)


def lotes(bronze, dataset, desde, hasta):
    """Ficheros de un dataset agrupados por mes de inicio de su ventana: {(año, mes): [(área, fichero)]}.

    Dentro de cada lote van del descargado antes al descargado después, para que al deduplicar gane el último.
    Mes, quincenas partidas y días sueltos del mismo mes caen en el mismo lote.
    """
    grupos = defaultdict(list)
    for f in (bronze / "entsoe" / dataset).glob("area=*/*=*/respuesta.*"):
        ini, fin = ventana(f.parent.name)
        if (desde is None or fin >= desde) and (hasta is None or ini <= hasta):
            grupos[(ini.year, ini.month)].append((f.stat().st_mtime, str(f), f.parent.parent.name.split("=", 1)[1], f))
    return {k: [(area, f) for _, _, area, f in sorted(v)] for k, v in sorted(grupos.items())}


def documentos(fichero):
    """XML que contiene un fichero de bronze: suelto, .gz o un ZIP con varios."""
    cuerpo = fichero.read_bytes()
    if fichero.name.endswith(".gz"):
        cuerpo = gzip.decompress(cuerpo)
    if cuerpo[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(cuerpo)) as z:
            return [z.read(n) for n in sorted(z.namelist())]
    return [cuerpo]


# ---------------------------------------------------------------- análisis del XML de ENTSO-E

def _local(el):
    return el.tag.split("}")[-1]


def parse_ts(texto):
    return dt.datetime.fromisoformat(texto.replace("Z", "+00:00")).astimezone(UTC)


def series(xml):
    """[(atributos de la TimeSeries, resolución, [(inicio_utc, campos del punto)])] con los huecos A03 rellenados.

    Los campos del punto son sus hijos simples (quantity, imbalance_Price.amount, imbalance_Price.category…).
    """
    raiz = ET.fromstring(xml)
    if _local(raiz) == "Acknowledgement_MarketDocument":
        return []
    salida = []
    for serie in (el for el in raiz if _local(el) == "TimeSeries"):
        attrs = {_local(h): (h.text or "").strip() for h in serie if len(h) == 0}
        attrs.update({_local(n): (n.text or "").strip() for h in serie if _local(h) == "MktPSRType" for n in h})
        for periodo in (el for el in serie if _local(el) == "Period"):
            intervalo = next(el for el in periodo if _local(el) == "timeInterval")
            inicio, fin = (parse_ts(next(el.text for el in intervalo if _local(el) == n)) for n in ("start", "end"))
            res = next(el.text for el in periodo if _local(el) == "resolution")
            paso = RESOLUCIONES[res]
            puntos = {}
            for p in (el for el in periodo if _local(el) == "Point"):
                campos = {_local(h): h.text for h in p if len(h) == 0}
                puntos[int(campos.pop("position"))] = campos
            filas, ultimo = [], None
            for pos in range(1, int((fin - inicio) / paso) + 1):
                ultimo = puntos.get(pos, ultimo)
                if ultimo is not None:          # si el Period empieza con hueco no se inventa nada
                    filas.append((inicio + (pos - 1) * paso, ultimo))
            salida.append((attrs, res, filas))
    return salida


# ---------------------------------------------------------------- escritura

def dia_mercado(ts_utc):
    return ts_utc.astimezone(BRUSELAS).date()


def base(area, ts, res, fichero, ahora, raiz):
    dia = dia_mercado(ts)
    return {"area": area, "interval_start_utc": ts, "market_date": dia, "market_year": dia.year, "resolution": res,
            "source_file": str(fichero.relative_to(raiz)), "loaded_at": ahora}


def guardar(silver, nombre, filas, esquema, clave):
    """MERGE idempotente en silver/<nombre> por la clave natural. Devuelve (insertadas, actualizadas)."""
    if not filas:
        return 0, 0
    tabla = pa.Table.from_pylist(filas, schema=esquema)
    ruta = silver / nombre
    if not (ruta / "_delta_log").exists():
        write_deltalake(str(ruta), tabla, partition_by=["market_year"])
        return tabla.num_rows, 0
    condicion = " AND ".join(f"t.{c} = s.{c}" for c in ["market_year", *clave])
    res = (DeltaTable(str(ruta)).merge(tabla, condicion, source_alias="s", target_alias="t")
           .when_matched_update_all().when_not_matched_insert_all().execute())
    return res.get("num_target_rows_inserted", 0), res.get("num_target_rows_updated", 0)


COMUNES = [("area", pa.string()), ("interval_start_utc", TS), ("market_date", pa.date32()), ("market_year", pa.int32()),
           ("resolution", pa.string())]
TRAZA = [("source_file", pa.string()), ("loaded_at", TS)]


# ---------------------------------------------------------------- tablas

def filas_precio_desvio(area, fichero, ahora, raiz):
    """A85. Dos series por área: precio para el BRP con excedente (A04) y con déficit (A05). Con precio único
    (single pricing) coinciden; donde no, la diferencia es información y se guardan las dos."""
    for xml in documentos(fichero):
        for attrs, res, puntos in series(xml):
            for ts, p in puntos:
                lado = LADO_PRECIO.get(p.get("imbalance_Price.category"), p.get("imbalance_Price.category"))
                yield (area, lado, ts), {**base(area, ts, res, fichero, ahora, raiz), "imbalance_side": lado,
                                         "imbalance_price": float(p["imbalance_Price.amount"]),
                                         "currency": attrs.get("currency_Unit.name")}


ESQUEMA_PRECIO = pa.schema(COMUNES + [("imbalance_side", pa.string()), ("imbalance_price", pa.float64()),
                                      ("currency", pa.string())] + TRAZA)


def filas_volumen_desvio(area, fichero, ahora, raiz):
    """A86. Volumen de desvío del sistema en MWh por intervalo, en series separadas por sentido (A01 / A02).
    Se guarda el sentido tal cual lo publica el TSO; el signo neto se decide en gold, validado contra Elia."""
    for xml in documentos(fichero):
        for attrs, res, puntos in series(xml):
            sentido = SENTIDO.get(attrs.get("flowDirection.direction"), attrs.get("flowDirection.direction"))
            for ts, p in puntos:
                yield (area, sentido, ts), {**base(area, ts, res, fichero, ahora, raiz), "direction": sentido,
                                            "imbalance_mwh": float(p["quantity"]),
                                            "unit": attrs.get("quantity_Measure_Unit.name")}


ESQUEMA_VOLUMEN = pa.schema(COMUNES + [("direction", pa.string()), ("imbalance_mwh", pa.float64()),
                                       ("unit", pa.string())] + TRAZA)

IGCC = "10Y1001C--00119X"   # área virtual de la plataforma de neteo


def filas_neteo(area, fichero, ahora, raiz):
    """B17 + A63. Energía neteada con IGCC, en MWh por intervalo. Cada serie va de connecting a acquiring:
    si quien adquiere es el área, importa de la plataforma; si adquiere la plataforma, el área exporta."""
    for xml in documentos(fichero):
        for attrs, res, puntos in series(xml):
            sentido = "export" if attrs.get("acquiring_Domain.mRID") == IGCC else "import"
            for ts, p in puntos:
                yield (area, sentido, ts), {**base(area, ts, res, fichero, ahora, raiz), "direction": sentido,
                                            "netted_mwh": float(p["quantity"])}


ESQUEMA_NETEO = pa.schema(COMUNES + [("direction", pa.string()), ("netted_mwh", pa.float64())] + TRAZA)

TECNOLOGIA = {"B16": "solar", "B18": "wind_offshore", "B19": "wind_onshore"}   # psrType


def filas_eolica_solar(columna):
    """A69 (previsión) y A75 (real). De la generación real solo interesan eólica y solar, y solo las series de
    generación (inBiddingZone); las de consumo propio (outBiddingZone) se descartan."""
    def generador(area, fichero, ahora, raiz):
        for xml in documentos(fichero):
            for attrs, res, puntos in series(xml):
                tecnologia = TECNOLOGIA.get(attrs.get("psrType"))
                if tecnologia is None or "inBiddingZone_Domain.mRID" not in attrs:
                    continue
                for ts, p in puntos:
                    yield (area, tecnologia, ts), {**base(area, ts, res, fichero, ahora, raiz),
                                                   "technology": tecnologia, columna: float(p["quantity"])}
    return generador


def esquema_eolica_solar(columna):
    return pa.schema(COMUNES + [("technology", pa.string()), (columna, pa.float64())] + TRAZA)


def filas_carga(columna):
    """A65: carga total de la zona, prevista el día antes (A01) o real (A16), en MW."""
    def generador(area, fichero, ahora, raiz):
        for xml in documentos(fichero):
            for attrs, res, puntos in series(xml):
                for ts, p in puntos:
                    yield (area, ts), {**base(area, ts, res, fichero, ahora, raiz), columna: float(p["quantity"])}
    return generador


def esquema_carga(columna):
    return pa.schema(COMUNES + [(columna, pa.float64())] + TRAZA)


# tabla silver → (dataset de bronze, generador de filas, esquema, clave natural)
TABLAS = {
    "imbalance_price":  ("imbalance_price", filas_precio_desvio, ESQUEMA_PRECIO,
                         ["area", "imbalance_side", "interval_start_utc"]),
    "imbalance_volume": ("imbalance_volume", filas_volumen_desvio, ESQUEMA_VOLUMEN,
                         ["area", "direction", "interval_start_utc"]),
    "netting":          ("netting_igcc", filas_neteo, ESQUEMA_NETEO, ["area", "direction", "interval_start_utc"]),
    "wind_solar_forecast": ("wind_solar_forecast", filas_eolica_solar("forecast_mw"), esquema_eolica_solar("forecast_mw"),
                            ["area", "technology", "interval_start_utc"]),
    "wind_solar_actual":   ("wind_solar_actual", filas_eolica_solar("actual_mw"), esquema_eolica_solar("actual_mw"),
                            ["area", "technology", "interval_start_utc"]),
    "load_forecast":    ("load_forecast", filas_carga("load_forecast_mw"), esquema_carga("load_forecast_mw"),
                         ["area", "interval_start_utc"]),
    "load_actual":      ("load_actual", filas_carga("load_mw"), esquema_carga("load_mw"), ["area", "interval_start_utc"]),
}


def construir(nombre, bronze, silver, raiz, desde, hasta, ahora):
    dataset, generador, esquema, clave = TABLAS[nombre]
    tabla = f"entsoe_{nombre}"
    total_ins = total_act = 0
    cobertura = defaultdict(lambda: defaultdict(set))    # área[/tecnología] → día de mercado → cuartos cubiertos
    grupo = lambda fila: fila["area"] + (f"/{fila['technology']}" if "technology" in fila else "")
    for (anio, mes), ficheros in lotes(bronze, dataset, desde, hasta).items():
        filas = {}
        for area, f in ficheros:
            for k, fila in generador(area, f, ahora, raiz):
                if (desde is None or fila["market_date"] >= desde) and (hasta is None or fila["market_date"] <= hasta):
                    filas[k] = fila          # el fichero más reciente pisa al anterior
        for fila in filas.values():
            n = int(RESOLUCIONES[fila["resolution"]] / CUARTO)
            cobertura[grupo(fila)][fila["market_date"]].update(
                fila["interval_start_utc"] + i * CUARTO for i in range(n))
        ins, act = guardar(silver, tabla, list(filas.values()), esquema, clave)
        total_ins, total_act = total_ins + ins, total_act + act
        print(f"  {tabla} {anio}-{mes:02d}: {len(filas):,} filas de {len(ficheros)} ficheros", flush=True)
    print(f"  {tabla}: {total_ins:,} insertadas, {total_act:,} actualizadas")
    return cobertura


def cuartos_del_dia(dia):
    """92, 96 o 100 según haya cambio de hora ese día."""
    ini = dt.datetime.combine(dia, dt.time(0), tzinfo=BRUSELAS).astimezone(UTC)
    fin = dt.datetime.combine(dia + dt.timedelta(days=1), dt.time(0), tzinfo=BRUSELAS).astimezone(UTC)
    return int((fin - ini) / CUARTO)


def informe_cobertura(nombre, cobertura):
    """Una línea por área: días con datos, primer y último día, y días con cuartos de hora sin cubrir."""
    for area in sorted(cobertura):
        dias = cobertura[area]
        incompletos = sorted(d for d, cuartos in dias.items() if len(cuartos) != cuartos_del_dia(d))
        ejemplo = (f" · p. ej. {incompletos[0]} ({len(dias[incompletos[0]])}/{cuartos_del_dia(incompletos[0])})"
                   if incompletos else "")
        print(f"  {nombre} {area:22} {len(dias):5} días · {min(dias)} → {max(dias)} · "
              f"{len(incompletos)} días incompletos{ejemplo}")


def main():
    p = argparse.ArgumentParser(description="Bronze → silver (Delta tables)")
    p.add_argument("--start", dest="desde", metavar="DATE", type=dt.date.fromisoformat, help="first market day, YYYY-MM-DD")
    p.add_argument("--end", dest="hasta", metavar="DATE", type=dt.date.fromisoformat, help="last market day, YYYY-MM-DD (inclusive)")
    p.add_argument("--only", dest="solo", choices=sorted(TABLAS), nargs="+", help="build only these tables")
    p.add_argument("--data-dir", dest="datos", type=Path, default=RAIZ / "data", help="data folder (default: ./data)")
    p.add_argument("--silver-dir", dest="silver", type=Path, help="write silver here instead of <data-dir>/silver")
    a = p.parse_args()
    bronze, silver = a.datos / "bronze", a.silver or a.datos / "silver"
    ahora = dt.datetime.now(UTC)
    print("Construyendo silver…")
    coberturas = {n: construir(n, bronze, silver, RAIZ, a.desde, a.hasta, ahora) for n in (a.solo or TABLAS)}
    print("\nCobertura (por cuartos de hora del día de mercado):")
    for n, c in coberturas.items():
        informe_cobertura(n, c)
    print(f"\nTerminado. Tablas en {silver}")


if __name__ == "__main__":
    sys.exit(main())
