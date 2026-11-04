#!/usr/bin/env python3
"""Recolector de datos abiertos (Geoportal Valencia ArcGIS y AEMET).

Uso: python recolector.py espiras tramos estado aire meteo aemet aemet_obs
Cada captura va a data/raw/<ruta>/fecha=AAAA-MM-DD/<nombre>_AAAAMMDDTHHMMSSZ.parquet
y cada ejecucion se apunta en data/raw/_registro/ejecuciones.csv.
Autonomo: no importa nada del resto del proyecto.
"""
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

AQUI = Path(__file__).resolve().parent
BASE_ARCGIS = "https://geoportal.valencia.es/server/rest/services/OPENDATA"
BASE_AEMET = "https://opendata.aemet.es/opendata/api"
REINTENTOS = 5
ESPERA_BASE = 2  # segundos; crece x2 en cada reintento
TIMEOUT = 60

# fuente -> (servicio/capa, ruta de salida, prefijo de fichero)
ARCGIS = {
    "espiras": ("Trafico/MapServer/208", "trafico/espiras", "espiras"),
    "tramos": ("Trafico/MapServer/188", "trafico/tramos", "tramos"),
    "estado": ("Trafico/MapServer/192", "trafico/estado", "estado"),
    "aire": ("MedioAmbiente/MapServer/156", "calidad_aire/estaciones", "aire"),
    "meteo": ("MedioAmbiente/MapServer/157", "meteo/municipal", "meteo"),
}
AEMET = {
    "aemet": ("/prediccion/especifica/municipio/diaria/46250", "aemet/prediccion_46250", "prediccion"),
    "aemet_obs": ("/observacion/convencional/datos/estacion/8416", "aemet/observacion_8416", "observacion"),
}
FUENTES = list(ARCGIS) + list(AEMET)
CAMPOS_REGISTRO = ["captura_utc", "fuente", "ok", "filas", "error"]

log = logging.getLogger("recolector")


def raiz_datos():
    return Path(os.environ.get("RECOLECTOR_DATA", AQUI.parent / "data" / "raw"))


def cargar_clave():
    for p in (AQUI.parent / ".env", AQUI / ".env"):
        if p.exists():
            load_dotenv(p)
    return os.environ.get("AEMET_API_KEY")


def get_json(url, params=None, headers=None, sesion=requests):
    """GET con reintentos (red, 429, 5xx) y espera creciente."""
    ultimo = None
    for intento in range(REINTENTOS):
        espera = ESPERA_BASE * 2 ** intento
        try:
            r = sesion.get(url, params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429 or r.status_code >= 500:
                ultimo = RuntimeError(f"HTTP {r.status_code}")
                ra = r.headers.get("Retry-After") if hasattr(r, "headers") else None
                if ra and str(ra).isdigit():
                    espera = max(espera, int(ra))
                log.warning("HTTP %s en intento %d/%d", r.status_code, intento + 1, REINTENTOS)
            else:
                r.raise_for_status()
                try:
                    r.encoding = "utf-8"
                    return r.json()
                except ValueError as e:
                    raise RuntimeError(f"respuesta no JSON: {e}")
        except (requests.ConnectionError, requests.Timeout) as e:
            ultimo = e
            log.warning("Error de red (%s) en intento %d/%d", type(e).__name__, intento + 1, REINTENTOS)
        if intento < REINTENTOS - 1:
            time.sleep(espera)
    raise RuntimeError(f"agotados {REINTENTOS} reintentos: {ultimo}")


def a_texto_si_anidado(v):
    return json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v


def descargar_arcgis(capa):
    url = f"{BASE_ARCGIS}/{capa}/query"
    filas, offset = [], 0
    while True:
        d = get_json(url, params={"where": "1=1", "outFields": "*", "f": "json",
                                  "outSR": 4326, "resultOffset": offset})
        if "error" in d:
            raise RuntimeError(f"error ArcGIS: {d['error']}")
        feats = d.get("features", [])
        for f in feats:
            fila = {k: a_texto_si_anidado(v) for k, v in (f.get("attributes") or {}).items()}
            g = f.get("geometry")
            fila["geometria"] = json.dumps(g) if g else None
            filas.append(fila)
        if d.get("exceededTransferLimit") and feats:
            offset += len(feats)
        else:
            return filas


def aemet_dos_pasos(ruta, clave):
    if not clave:
        raise RuntimeError("falta AEMET_API_KEY en .env")
    r1 = get_json(BASE_AEMET + ruta, headers={"api_key": clave})
    if r1.get("estado") != 200 or "datos" not in r1:
        raise RuntimeError(f"AEMET paso 1: estado={r1.get('estado')} {r1.get('descripcion')}")
    return get_json(r1["datos"])


def _periodo_0024(lista):
    for e in lista or []:
        if e.get("periodo") == "00-24":
            return e.get("value")
    return None


def aplanar_prediccion(datos):
    filas = []
    for item in datos if isinstance(datos, list) else [datos]:
        for dia in (item.get("prediccion") or {}).get("dia", []):
            t = dia.get("temperatura") or {}
            filas.append({
                "municipio": item.get("nombre"),
                "id_municipio": item.get("id"),
                "elaborado": item.get("elaborado"),
                "fecha": dia.get("fecha"),
                "temperatura_maxima": t.get("maxima"),
                "temperatura_minima": t.get("minima"),
                "probabilidad_precipitacion": _periodo_0024(dia.get("probPrecipitacion")),
            })
    return filas


def aplanar_observacion(datos):
    return [{k: a_texto_si_anidado(v) for k, v in f.items()} for f in datos]


def escribir_parquet(filas, ruta, prefijo, ahora):
    ts = ahora.strftime("%Y%m%dT%H%M%SZ")
    for f in filas:
        f["captura_utc"] = ahora.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        tabla = pa.Table.from_pylist(filas)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        # tipos mezclados en una columna: se pasa a texto
        cols = {k for f in filas for k in f}
        tabla = pa.Table.from_pylist([
            {k: (None if f.get(k) is None else str(f.get(k))) for k in cols} for f in filas])
    dest = raiz_datos() / ruta / f"fecha={ahora:%Y-%m-%d}"
    dest.mkdir(parents=True, exist_ok=True)
    fichero = dest / f"{prefijo}_{ts}.parquet"
    pq.write_table(tabla, fichero, compression="snappy")
    return fichero


def registrar(ahora, fuente, ok, filas, error):
    d = raiz_datos() / "_registro"
    d.mkdir(parents=True, exist_ok=True)
    f = d / "ejecuciones.csv"
    nuevo = not f.exists()
    with open(f, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if nuevo:
            w.writerow(CAMPOS_REGISTRO)
        w.writerow([ahora.strftime("%Y-%m-%dT%H:%M:%SZ"), fuente, str(ok).lower(), filas,
                    (error or "")[:300].replace("\n", " ")])


def recolectar(fuente):
    ahora = datetime.now(timezone.utc)
    try:
        if fuente in ARCGIS:
            capa, ruta, prefijo = ARCGIS[fuente]
            filas = descargar_arcgis(capa)
        else:
            api, ruta, prefijo = AEMET[fuente]
            datos = aemet_dos_pasos(api, cargar_clave())
            filas = aplanar_prediccion(datos) if fuente == "aemet" else aplanar_observacion(datos)
        if filas:
            fichero = escribir_parquet(filas, ruta, prefijo, ahora)
            log.info("%s: %d filas -> %s", fuente, len(filas), fichero)
        else:
            log.warning("%s: sin filas, no se escribe parquet", fuente)
        registrar(ahora, fuente, True, len(filas), "")
        return True
    except Exception as e:  # fallo independiente por fuente
        log.error("%s: FALLO %s", fuente, e)
        try:
            registrar(ahora, fuente, False, 0, f"{type(e).__name__}: {e}")
        except Exception:
            log.exception("no se pudo escribir el registro")
        return False


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    fuentes = (sys.argv[1:] if argv is None else argv) or FUENTES
    desconocidas = [f for f in fuentes if f not in FUENTES]
    if desconocidas:
        print(f"Fuentes desconocidas: {desconocidas}. Validas: {FUENTES}", file=sys.stderr)
        return 2
    resultados = {f: recolectar(f) for f in fuentes}
    fallos = [f for f, ok in resultados.items() if not ok]
    log.info("Resumen: %d ok, %d fallos %s", len(resultados) - len(fallos), len(fallos), fallos)
    return 1 if fallos else 0


if __name__ == "__main__":
    sys.exit(main())
