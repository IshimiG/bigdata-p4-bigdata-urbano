"""Descarga el historico horario de calidad del aire de Valencia (CKAN) a Parquet.

Portal: https://opendata.vlci.valencia.es (API CKAN, datastore_search).
Todos los valores se guardan como texto tal cual vienen; solo se normalizan
los nombres de columna. La limpieza de fechas es cosa del Bloque 4.

Salida: data/raw/calidad_aire/historico/origen=<nombre>/part-0000.parquet
"""
import argparse
import re
import shutil
import sys
import time
import unicodedata
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

BASE_URL = "https://opendata.vlci.valencia.es/api/3/action/datastore_search"
# paquete CKAN -> resource_id (localizados con package_search q='calidad aire')
ORIGENES = {
    # paquete hourly-air-quality-data-since-2016 (recurso CSV en castellano)
    "2016_2021": "4be7248b-9597-4017-89af-82a9b6e2382f",
    # paquete rvvcca_d_horarios_2021-2022
    "2021_2022": "19b5a1a7-5888-4d3e-b69b-3c1436d64e6e",
}
DESTINO = Path(__file__).resolve().parent.parent / "data/raw/calidad_aire/historico"
PAGINA = 10000


def normalizar_columna(nombre):
    """'PM2.5' -> 'pm2_5', 'Dia de la semana' -> 'dia_de_la_semana', '_id' -> 'ckan_id'."""
    if nombre == "_id":
        return "ckan_id"
    s = unicodedata.normalize("NFKD", nombre)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or "col"


def normalizar_columnas(nombres):
    """Normaliza una lista de nombres evitando colisiones (sufijo _2, _3...)."""
    vistos, salida = set(), []
    for n in nombres:
        base = normalizar_columna(n)
        nuevo, i = base, 2
        while nuevo in vistos:
            nuevo, i = f"{base}_{i}", i + 1
        vistos.add(nuevo)
        salida.append(nuevo)
    return salida


def a_texto(v):
    return None if v is None else str(v)


def pedir_pagina(resource_id, offset, limit=PAGINA, reintentos=5, sesion=requests, espera=2.0):
    ultimo = None
    for intento in range(reintentos):
        try:
            r = sesion.get(BASE_URL, params={"resource_id": resource_id, "limit": limit,
                                             "offset": offset}, timeout=120)
            r.raise_for_status()
            d = r.json()
            if not d.get("success"):
                raise RuntimeError(f"CKAN success=false: {d}")
            return d["result"]
        except Exception as e:  # red, HTTP, JSON
            ultimo = e
            time.sleep(espera * (2 ** intento))
    raise RuntimeError(f"Fallo tras {reintentos} intentos (offset {offset}): {ultimo}")


def descargar_origen(nombre, resource_id, destino=DESTINO, forzar=False, pagina=PAGINA,
                     sesion=requests, espera=2.0):
    """Devuelve (filas_escritas, total_ckan). Idempotente salvo forzar."""
    carpeta = Path(destino) / f"origen={nombre}"
    fichero = carpeta / "part-0000.parquet"
    if fichero.exists() and not forzar:
        filas = pq.ParquetFile(fichero).metadata.num_rows
        print(f"[{nombre}] ya existe ({filas} filas); usa --forzar para repetir")
        return filas, None
    if carpeta.exists():
        shutil.rmtree(carpeta)
    carpeta.mkdir(parents=True)
    tmp = fichero.with_suffix(".parquet.tmp")
    writer, offset, total, cols = None, 0, None, None
    try:
        while True:
            res = pedir_pagina(resource_id, offset, pagina, sesion=sesion, espera=espera)
            total = res["total"]
            campos = [f["id"] for f in res["fields"]]
            registros = res["records"]
            if writer is None:
                cols = campos
                nuevos = normalizar_columnas(cols)
                schema = pa.schema([(c, pa.string()) for c in nuevos])
                writer = pq.ParquetWriter(tmp, schema, compression="snappy")
            if registros:
                tabla = pa.table({n: pa.array([a_texto(r.get(o)) for r in registros], pa.string())
                                  for o, n in zip(cols, schema.names)}, schema=schema)
                writer.write_table(tabla)
            offset += len(registros)
            print(f"[{nombre}] {offset}/{total}", flush=True)
            if not registros or offset >= total:
                break
        writer.close()
        writer = None
        tmp.replace(fichero)
    except BaseException:
        if writer is not None:
            writer.close()
        tmp.unlink(missing_ok=True)
        shutil.rmtree(carpeta, ignore_errors=True)
        raise
    return offset, total


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--forzar", action="store_true", help="repetir descarga aunque existan datos")
    ap.add_argument("--destino", default=str(DESTINO))
    args = ap.parse_args(argv)
    ok = True
    for nombre, rid in ORIGENES.items():
        filas, total = descargar_origen(nombre, rid, args.destino, args.forzar)
        if total is not None and filas != total:
            print(f"[{nombre}] ERROR: {filas} filas != total CKAN {total}", file=sys.stderr)
            ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
