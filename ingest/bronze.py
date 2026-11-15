#!/usr/bin/env python3
"""Ingesta bronze del observatorio de balance: descarga del ENTSO-E Transparency Platform y guarda la respuesta cruda.

Cada descarga queda en  data/bronze/entsoe/<dataset>/area=<AREA>/<dia=AAAA-MM-DD | mes=AAAA-MM>/respuesta.<xml|zip>[.gz]
y se anota en         data/bronze/_registro_cargas.jsonl  (una línea JSON por descarga).
Si ENTSO-E rechaza una ventana por exceder su límite de documentos por petición (pasa con las reservas contratadas
por meses), la ventana se parte en dos mitades (carpeta rango=AAAA-MM-DD_AAAA-MM-DD) hasta que cabe.

El "día" es el día de mercado en hora de Bruselas (00:00–24:00 locales). Los datasets, áreas y parámetros vienen del
catálogo explorado el 14-15/11/2026 (docs/conceptos-balance.md, sección 8). Rasgos de esta ingesta:

- A85, A86, A81 y B17 responden un ZIP con uno o varios XML; A84, A24, A69, A74 y A65 un XML suelto. Se guarda tal
  cual (el ZIP ya está comprimido; el XML del CBMP a 4 s, ~3 MB/día, se guarda gzip).
- Los endpoints de balance tardan 10-30 s y ENTSO-E corta la conexión a veces: timeout largo, reintentos, y un fallo
  nunca para la carga (queda anotado como error y se repite en la siguiente ejecución).
- El CBMP (cbmp_afrr) y el intercambio aFRR (exchanged_afrr) pesan 100 MB y 90 s por mes y área: por meses van con
  un solo hilo y timeout de 5 min; su ventana de revisión es de 2 días (no se revisan), el resto 7.
- Alemania: el precio de desvío (reBAP) sale solo con el EIC de país; el resto del balance, por área de control;
  Amprion no publica A84. La tabla DATASETS dice qué áreas pide cada dataset.

Uso (desde la carpeta del proyecto, con el entorno virtual activado):
  python3 ingest/bronze.py --start 2026-10-11 --end 2026-10-12                 todos los datasets
  python3 ingest/bronze.py --start 2026-10-11 --end 2026-10-12 --only imbalance_price
  python3 ingest/bronze.py --start 2025-10-01 --end 2026-10-31 --chunk month   backfill: un mes por petición
  python3 ingest/bronze.py --daily                                             carga diaria: últimos 7 días
  python3 ingest/bronze.py --start 2026-10-11 --end 2026-10-12 --area BE --force

Solo usa la biblioteca estándar de Python (+ truststore si está instalado).
"""
import argparse
import calendar
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from zoneinfo import ZoneInfo

try:  # certificados del llavero de macOS (antivirus o VPN que inspeccionan https)
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    print("Aviso: truststore no está instalado en este entorno; si hay un antivirus o proxy que inspecciona https, "
          "todas las descargas fallarán con CERTIFICATE_VERIFY_FAILED. Arreglo: pip install truststore", file=sys.stderr)

RAIZ = Path(__file__).resolve().parent.parent
BRONZE = RAIZ / "data" / "bronze"
REGISTRO = BRONZE / "_registro_cargas.jsonl"
BRUSELAS = ZoneInfo("Europe/Brussels")
AGENTE = {"User-Agent": "balancing-observatory/0.1 (portfolio project)"}  # la pasarela de ENTSO-E rechaza el de Python
ENTSOE_URL = "https://web-api.tp.entsoe.eu/api"

# Áreas. El balance se publica por área de control (LFC area); precios y previsiones por zona de oferta.
AREAS = {
    "BE":            "10YBE----------2",
    "NL":            "10YNL----------L",
    "FR":            "10YFR-RTE------C",
    "AT":            "10YAT-APG------L",
    "CZ":            "10YCZ-CEPS-----N",
    "DK1":           "10YDK-1--------W",
    "DK2":           "10YDK-2--------M",
    "DE_50HZ":       "10YDE-VE-------2",
    "DE_AMPRION":    "10YDE-RWENET---I",
    "DE_TENNET":     "10YDE-EON------1",
    "DE_TRANSNETBW": "10YDE-ENBW-----N",
    "DE":            "10Y1001A1001A83F",   # país: único EIC bajo el que sale el reBAP
    "DE_LU":         "10Y1001A1001A82H",   # zona de oferta: previsiones y carga
}
ZONAS = ["BE", "NL", "FR", "AT", "CZ", "DK1", "DK2", "DE_LU"]                      # previsiones y reales
AREAS_BALANCE = ["BE", "NL", "FR", "AT", "CZ", "DK1", "DK2", "DE_50HZ", "DE_AMPRION", "DE_TENNET", "DE_TRANSNETBW"]
AREAS_AFRR = [a for a in AREAS_BALANCE if a != "DE_AMPRION"]                        # Amprion no publica A84
AREAS_DESVIO = ["BE", "NL", "FR", "AT", "CZ", "DK1", "DK2", "DE"]                    # A85: Alemania por país

# dataset → (parámetros fijos, nombre del parámetro de área, áreas, días de revisión, ¿gzip?)
# Los documentType/processType están probados uno a uno (docs/catalogo_balancing.txt).
DATASETS = {
    # ---- balance (artículos 17.1 del Reg. 543/2013 y 12.3 de la EB GL)
    "imbalance_price":     ({"documentType": "A85"},                        "controlArea_Domain", AREAS_DESVIO,  7, False),
    "imbalance_volume":    ({"documentType": "A86"},                        "controlArea_Domain", AREAS_BALANCE, 7, False),
    "cbmp_afrr":           ({"documentType": "A84", "processType": "A67"},  "controlArea_Domain", AREAS_AFRR,    2, True),
    "activation_price":    ({"documentType": "A84", "processType": "A16"},  "controlArea_Domain", AREAS_AFRR,    7, False),
    "activation_price_ls": ({"documentType": "A84", "processType": "A68"},  "controlArea_Domain", AREAS_AFRR,    7, False),
    "aggregated_bids_afrr": ({"documentType": "A24", "processType": "A51"}, "area_Domain",        AREAS_BALANCE, 7, False),
    "aggregated_bids_mfrr": ({"documentType": "A24", "processType": "A47"}, "area_Domain",        AREAS_BALANCE, 7, False),
    "aggregated_bids_mfrr_sa": ({"documentType": "A24", "processType": "A60"}, "area_Domain",     AREAS_BALANCE, 7, False),
    "aggregated_bids_mfrr_da": ({"documentType": "A24", "processType": "A61"}, "area_Domain",     AREAS_BALANCE, 7, False),
    "contracted_reserves": ({"documentType": "A81", "businessType": "B95", "type_MarketAgreement.Type": "A01"},
                            "controlArea_Domain", AREAS_BALANCE, 7, False),
    "netting_igcc":        ({"documentType": "B17", "processType": "A63"},  "controlArea_Domain", AREAS_BALANCE, 7, False),
    "exchanged_afrr":      ({"documentType": "B17", "processType": "A51"},  "controlArea_Domain", AREAS_AFRR,    2, False),
    # ---- previsión y real (artículos 6.1, 14.1 y 16.1)
    "wind_solar_forecast": ({"documentType": "A69", "processType": "A01"},  "in_Domain",            ZONAS, 7, False),
    "wind_solar_actual":   ({"documentType": "A74", "processType": "A16"},  "in_Domain",            ZONAS, 7, False),
    "load_forecast":       ({"documentType": "A65", "processType": "A01"},  "outBiddingZone_Domain", ZONAS, 7, False),
    "load_actual":         ({"documentType": "A65", "processType": "A16"},  "outBiddingZone_Domain", ZONAS, 7, False),
}
PESADOS = {"cbmp_afrr", "exchanged_afrr"}   # 4 s de resolución: un hilo y timeout largo cuando van por meses


# ---------------------------------------------------------------- utilidades

def leer_token():
    if os.environ.get("ENTSOE_TOKEN"):
        return os.environ["ENTSOE_TOKEN"].strip()
    env = RAIZ / ".env"
    if env.exists():
        for linea in env.read_text().splitlines():
            if linea.startswith("ENTSOE_TOKEN="):
                return linea.split("=", 1)[1].strip()
    sys.exit("No encuentro el token de ENTSO-E: falta ENTSOE_TOKEN en .env")


def ventana_utc(dia):
    """Día de mercado en Bruselas → (inicio, fin) en UTC."""
    inicio = dt.datetime.combine(dia, dt.time(0), tzinfo=BRUSELAS)
    fin = dt.datetime.combine(dia + dt.timedelta(days=1), dt.time(0), tzinfo=BRUSELAS)
    return inicio.astimezone(dt.timezone.utc), fin.astimezone(dt.timezone.utc)


def descargar(url, intentos=4, timeout=180):
    """GET con reintentos ante 429, 5xx, timeouts y errores de red. Devuelve (código, bytes); código 0 = sin respuesta."""
    espera = 10
    for intento in range(1, intentos + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=AGENTE), timeout=timeout) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            cuerpo = e.read()
            if e.code in (429, 500, 502, 503, 504, 599) and intento < intentos:
                time.sleep(60 if e.code == 429 else espera * intento)   # 429: límite por minuto de ENTSO-E
                continue
            return e.code, cuerpo
        except (urllib.error.URLError, TimeoutError, OSError) as e:   # red caída, lectura cortada, timeout
            if "CERTIFICATE_VERIFY_FAILED" in str(e):   # reintentar no lo arregla: falla rápido y explica
                return 0, (f"{e} · instala truststore en el entorno virtual (pip install -r requirements.txt) "
                           "o revisa el proxy/antivirus que inspecciona https").encode()
            if intento < intentos:
                time.sleep(espera * intento)
                continue
            return 0, f"{type(e).__name__}: {e}".encode()


def sin_token(url):
    partes = urllib.parse.urlsplit(url)
    consulta = [(k, v) for k, v in urllib.parse.parse_qsl(partes.query) if k != "securityToken"]
    return urllib.parse.urlunsplit(partes._replace(query=urllib.parse.urlencode(consulta)))


_cerrojo = threading.Lock()


def anotar(entrada):
    REGISTRO.parent.mkdir(parents=True, exist_ok=True)
    with _cerrojo, REGISTRO.open("a", encoding="utf-8") as f:   # varias descargas a la vez escriben aquí
        f.write(json.dumps(entrada, ensure_ascii=False) + "\n")


def leer_registro():
    """Último estado anotado por fichero: {ruta relativa: estado}. Una pasada, para no releer el log por descarga."""
    estados = {}
    if REGISTRO.exists():
        for linea in REGISTRO.read_text(encoding="utf-8").splitlines():
            e = json.loads(linea)
            if e.get("fichero"):
                estados[e["fichero"]] = e["estado"]
            if e["estado"] == "dividido":   # ventana demasiado grande para la API: ir directo a las mitades
                estados[f"{e['dataset']}/{e['area']}/{e['dia_mercado']}_{e['dia_mercado_fin']}"] = "dividido"

    return estados


def ruta(dataset, area, etiqueta, zip_, gz):
    ext = ("zip" if zip_ else "xml") + (".gz" if gz else "")
    return BRONZE / "entsoe" / dataset / f"area={area}" / etiqueta / f"respuesta.{ext}"


def ventanas(desde, hasta, bloque):
    """(etiqueta de carpeta, primer día, último día). Por días: dia=AAAA-MM-DD; por meses: mes=AAAA-MM."""
    if bloque == "dia":
        d = desde
        while d <= hasta:
            yield f"dia={d.isoformat()}", d, d
            d += dt.timedelta(days=1)
        return
    d = desde.replace(day=1)
    while d <= hasta:
        ultimo = d.replace(day=calendar.monthrange(d.year, d.month)[1])
        ini, fin = max(d, desde), min(ultimo, hasta)
        sufijo = "" if (ini == d and fin == ultimo) else f"_{ini.day:02d}-{fin.day:02d}"  # mes incompleto
        yield f"mes={d:%Y-%m}{sufijo}", ini, fin
        d = ultimo + dt.timedelta(days=1)


# ---------------------------------------------------------------- análisis de la respuesta

def _local(el):
    return el.tag.split("}")[-1]


def documentos(cuerpo):
    """(es_zip, [xml, ...]). ENTSO-E devuelve ZIP con varios XML en A85, A86, A81 y B17."""
    if cuerpo[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(cuerpo)) as z:
            return True, [z.read(n) for n in z.namelist()]
    return False, [cuerpo]


def analizar(cuerpo):
    """(estado, es_zip, nº TimeSeries, nº puntos, detalle). ENTSO-E responde 200 con un Acknowledgement si no hay datos."""
    es_zip, xmls = documentos(cuerpo)
    series = puntos = 0
    motivo = ""
    for x in xmls:
        try:
            raiz = ET.fromstring(x)
        except ET.ParseError as e:
            return "error", es_zip, 0, 0, f"XML ilegible: {e}"
        if _local(raiz) == "Acknowledgement_MarketDocument":
            motivo = " ".join(el.text.strip() for el in raiz.iter() if _local(el) == "text" and el.text)
            continue
        for ts in raiz:
            if _local(ts) == "TimeSeries":
                series += 1
                puntos += sum(1 for el in ts.iter() if _local(el) == "Point")
    if series == 0:
        return "sin_datos", es_zip, 0, 0, motivo[:200]
    return "ok", es_zip, series, puntos, ""


def motivo_rechazo(cuerpo):
    """Texto del motivo (Reason/text) de un Acknowledgement de error; si no es XML, el principio del cuerpo."""
    try:
        raiz = ET.fromstring(cuerpo)
        return " ".join(el.text.strip() for el in raiz.iter() if _local(el) == "text" and el.text)
    except ET.ParseError:
        return cuerpo[:200].decode("utf-8", "replace")


def mitades(dia_ini, dia_fin):
    """Parte una ventana de varios días en dos; cada mitad con su etiqueta de carpeta."""
    medio = dia_ini + (dia_fin - dia_ini) // 2
    for a, b in ((dia_ini, medio), (medio + dt.timedelta(days=1), dia_fin)):
        yield (f"dia={a.isoformat()}" if a == b else f"rango={a.isoformat()}_{b.isoformat()}"), a, b


# ---------------------------------------------------------------- carga

def cargar(dataset, area, ventana, token, forzar, estados):
    params, param_area, _, _, gz = DATASETS[dataset]
    etiqueta, dia_ini, dia_fin = ventana
    # el fichero puede ser xml o zip según responda ENTSO-E: se mira si ya existe cualquiera de los dos
    candidatos = [ruta(dataset, area, etiqueta, z, gz) for z in (False, True)]
    if not forzar and any(estados.get(str(c.relative_to(RAIZ))) in ("ok", "sin_datos") for c in candidatos):
        print(f"  = {dataset}/{area} {etiqueta} · ya estaba")
        return
    clave_ventana = f"{dataset}/{area}/{dia_ini.isoformat()}_{dia_fin.isoformat()}"
    if estados.get(clave_ventana) == "dividido":   # ya se sabe que esta ventana excede el límite de la API
        for mitad in mitades(dia_ini, dia_fin):
            cargar(dataset, area, mitad, token, forzar, estados)
        return
    inicio, fin = ventana_utc(dia_ini)[0], ventana_utc(dia_fin)[1]
    q = {"securityToken": token, **params, param_area: AREAS[area],
         "periodStart": inicio.strftime("%Y%m%d%H%M"), "periodEnd": fin.strftime("%Y%m%d%H%M")}
    url = f"{ENTSOE_URL}?{urllib.parse.urlencode(q)}"
    t0 = time.time()
    codigo, cuerpo = descargar(url, timeout=600 if (dataset in PESADOS and dia_fin != dia_ini) else 180)
    if codigo == 200:
        estado, es_zip, series, puntos, detalle = analizar(cuerpo)
    else:
        estado, es_zip, series, puntos = "error", False, 0, 0
        detalle = f"HTTP {codigo}: {motivo_rechazo(cuerpo)}"
        # ENTSO-E limita el nº de documentos por petición (400 "exceeds allowed limit"): se parte la ventana en dos
        if codigo == 400 and dia_fin > dia_ini and ("exceeds" in detalle or "allowed" in detalle):
            estado = "dividido"
        # los datasets a 4 s a veces revientan por dentro en ENTSO-E ("I/O error") con un mes entero: probar con menos
        elif dataset in PESADOS and dia_fin > dia_ini and "I/O error" in detalle:
            estado = "dividido"
    fichero = ruta(dataset, area, etiqueta, es_zip, gz)
    if estado in ("ok", "sin_datos"):
        fichero.parent.mkdir(parents=True, exist_ok=True)
        fichero.write_bytes(gzip.compress(cuerpo) if gz else cuerpo)
    anotar({
        "cargado_en": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "fuente": "entsoe", "dataset": dataset, "area": area,
        "dia_mercado": dia_ini.isoformat(), "dia_mercado_fin": dia_fin.isoformat(),
        "url": sin_token(url), "http": codigo, "estado": estado, "series": series, "puntos": puntos,
        "bytes": len(cuerpo), "sha256": hashlib.sha256(cuerpo).hexdigest(),
        "fichero": str(fichero.relative_to(RAIZ)) if estado in ("ok", "sin_datos") else None,
        "duracion_s": round(time.time() - t0, 1), "detalle": detalle[:300],
    })
    simbolo = {"ok": "✓", "sin_datos": "·", "error": "✗", "dividido": "÷"}[estado]
    print(f"  {simbolo} {dataset}/{area} {etiqueta} · {estado} · {series} series · {puntos} puntos · "
          f"{len(cuerpo):,} bytes · {time.time() - t0:.0f} s" + (f" · {detalle[:120]}" if estado != "ok" and detalle else ""))
    time.sleep(0.5)   # ENTSO-E permite 400 peticiones/minuto; aquí no pasamos de ~20
    if estado == "dividido":
        for mitad in mitades(dia_ini, dia_fin):
            cargar(dataset, area, mitad, token, forzar, estados)


# ---------------------------------------------------------------- principal

def main():
    p = argparse.ArgumentParser(description="Bronze ingestion of ENTSO-E balancing and forecast data, per market day (Brussels time)")
    p.add_argument("--start", dest="desde", metavar="DATE", type=dt.date.fromisoformat, help="first market day, YYYY-MM-DD")
    p.add_argument("--end", dest="hasta", metavar="DATE", type=dt.date.fromisoformat, help="last market day, YYYY-MM-DD (inclusive)")
    p.add_argument("--daily", dest="diario", action="store_true",
                   help="daily run: from --revision-days days ago until yesterday (complete days)")
    p.add_argument("--revision-days", dest="revisar", type=int, default=7, metavar="DAYS",
                   help="re-download the last DAYS days even if already loaded (0 = never); heavy datasets use min(DAYS, 2)")
    p.add_argument("--only", dest="solo", choices=sorted(DATASETS), nargs="+", help="load only these datasets")
    p.add_argument("--area", choices=sorted(AREAS), nargs="+", help="load only these areas")
    p.add_argument("--force", dest="forzar", action="store_true", help="re-download even if already loaded")
    p.add_argument("--chunk", dest="bloque", choices=["day", "month"], default="day",
                   help="request size: one day (daily run) or one month (backfill)")
    p.add_argument("--threads", type=int, default=3, help="parallel downloads (heavy monthly datasets always use 1)")
    a = p.parse_args()
    a.bloque = {"day": "dia", "month": "mes"}[a.bloque]
    hoy = dt.datetime.now(BRUSELAS).date()
    if a.diario:
        a.desde, a.hasta = hoy - dt.timedelta(days=max(a.revisar, 1)), hoy - dt.timedelta(days=1)
    if not (a.desde and a.hasta):
        sys.exit("give --start and --end, or --daily")
    if a.hasta < a.desde:
        sys.exit("--end cannot be earlier than --start")

    token = leer_token()
    estados = leer_registro()
    datasets = a.solo or list(DATASETS)
    for dataset in datasets:
        params, _, areas, revision, _ = DATASETS[dataset]
        areas = [x for x in areas if not a.area or x in a.area]
        if not areas:
            continue
        dias_revision = min(a.revisar, revision)
        limite = hoy - dt.timedelta(days=dias_revision)
        print(f"\n{dataset} · {params} · {len(areas)} áreas · revisión {dias_revision} días")
        trabajos = [(area, v) for v in ventanas(a.desde, a.hasta, a.bloque) for area in areas]
        hilos = 1 if (dataset in PESADOS and a.bloque == "mes") else a.threads
        with ThreadPoolExecutor(max_workers=hilos) as pool:
            list(pool.map(lambda t: cargar(dataset, t[0], t[1], token, a.forzar or t[1][2] > limite, estados), trabajos))

    # último estado por (dataset, área, ventana): un error ya corregido en una ejecución posterior no cuenta
    ultimo = {}
    if REGISTRO.exists():
        for linea in REGISTRO.read_text(encoding="utf-8").splitlines():
            e = json.loads(linea)
            ultimo[(e["dataset"], e["area"], e["dia_mercado"], e["dia_mercado_fin"])] = e
    errores = sum(1 for e in ultimo.values()
                  if e["estado"] == "error" and e["dataset"] in datasets
                  and a.desde.isoformat() <= e["dia_mercado"] <= a.hasta.isoformat())
    print(f"\nTerminado. Registro: {REGISTRO.relative_to(RAIZ)}"
          + (f" · {errores} errores en este rango (revisa las líneas con ✗; se reintentan en la próxima ejecución)" if errores else " · sin errores"))


if __name__ == "__main__":
    main()
