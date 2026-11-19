#!/usr/bin/env python3
"""Tipos de cambio de referencia del BCE (euro frente a otras monedas), para pasar a euros los precios que un TSO
publica en su moneda. Hoy solo hace falta la corona checa: ČEPS publica el precio de desvío en CZK.

Descarga la serie diaria completa desde 2022-12-01 y la guarda cruda en
  data/bronze/ecb/exr/currency=<MONEDA>/respuesta.csv
Cada ejecución sustituye el fichero: la serie es pequeña (un valor por día hábil) y el BCE no la revisa.

Uso:  python3 ingest/fx.py            (CZK)
      python3 ingest/fx.py --currency CZK DKK
"""
import argparse
import sys
import urllib.request
from pathlib import Path

try:  # certificados del llavero de macOS (antivirus o VPN que inspeccionan https)
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

RAIZ = Path(__file__).resolve().parent.parent
DESTINO = RAIZ / "data" / "bronze" / "ecb" / "exr"
URL = "https://data-api.ecb.europa.eu/service/data/EXR/D.{moneda}.EUR.SP00.A?format=csvdata&startPeriod=2022-12-01"


def main():
    p = argparse.ArgumentParser(description="ECB euro reference exchange rates → bronze")
    p.add_argument("--currency", nargs="+", default=["CZK"], help="ISO currency codes (default: CZK)")
    a = p.parse_args()
    for moneda in a.currency:
        peticion = urllib.request.Request(URL.format(moneda=moneda), headers={"User-Agent": "balancing-observatory/0.1"})
        with urllib.request.urlopen(peticion, timeout=60) as r:
            cuerpo = r.read()
        lineas = cuerpo.decode("utf-8").splitlines()
        if len(lineas) < 2 or "TIME_PERIOD" not in lineas[0] or "OBS_VALUE" not in lineas[0]:
            sys.exit(f"Respuesta inesperada del BCE para {moneda}: {cuerpo[:200]!r}")
        fichero = DESTINO / f"currency={moneda}" / "respuesta.csv"
        fichero.parent.mkdir(parents=True, exist_ok=True)
        fichero.write_bytes(cuerpo)
        print(f"  ✓ {moneda}: {len(lineas) - 1} días · {fichero.relative_to(RAIZ)}")


if __name__ == "__main__":
    main()
