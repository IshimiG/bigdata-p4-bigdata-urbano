
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv


# Rutas del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "prediccion_paterna.json"

# Configuración AEMET
API_BASE_URL = "https://opendata.aemet.es/opendata/api"
MUNICIPIO = "46190"  # Paterna, Valencia
ENDPOINT = (
    f"{API_BASE_URL}/prediccion/"
    f"especifica/municipio/diaria/{MUNICIPIO}"
)

TIMEOUT = 30


def descargar_prediccion() -> None:
    """Descarga y guarda la predicción diaria de Paterna."""

    load_dotenv(PROJECT_ROOT / ".env")

    api_key = os.getenv("AEMET_API_KEY")

    if not api_key:
        raise RuntimeError(
            "No se ha encontrado AEMET_API_KEY en el archivo .env"
        )

    headers = {"api_key": api_key}

    with requests.Session() as session:
        # 1. Solicitar el enlace de descarga a AEMET
        response = session.get(
            ENDPOINT,
            headers=headers,
            timeout=TIMEOUT,
        )

        # AEMET suele devolver el motivo del error en el JSON (401, 404, 429...)
        if response.status_code != 200:
            try:
                descripcion = response.json().get("descripcion")
            except (ValueError, AttributeError):
                descripcion = None

            if descripcion:
                raise RuntimeError(
                    f"AEMET ha devuelto un error {response.status_code}: "
                    f"{descripcion}"
                )

        response.raise_for_status()

        try:
            respuesta_api = response.json()
        except ValueError as exc:
            raise RuntimeError(
                "AEMET no ha devuelto un JSON válido en la "
                "respuesta inicial."
            ) from exc

        if respuesta_api.get("estado") != 200:
            raise RuntimeError(
                "AEMET no ha confirmado la consulta: "
                f"{respuesta_api.get('descripcion')} ({respuesta_api})"
            )

        url_datos = respuesta_api.get("datos")

        if not url_datos:
            raise RuntimeError(
                "La respuesta de AEMET no contiene el enlace 'datos'."
            )

        # 2. Descargar los datos meteorológicos completos
        respuesta_datos = session.get(
            url_datos,
            timeout=TIMEOUT,
        )
        respuesta_datos.raise_for_status()

        # AEMET sirve estos datos en ISO-8859-15; si la cabecera no indica
        # charset, requests puede asumir UTF-8 y estropear las tildes.
        content_type = respuesta_datos.headers.get("Content-Type", "")
        if "charset" not in content_type.lower():
            respuesta_datos.encoding = "ISO-8859-15"

        try:
            datos = json.loads(respuesta_datos.text)
        except ValueError as exc:
            raise RuntimeError(
                "El enlace 'datos' no ha devuelto un JSON válido."
            ) from exc

    # 3. Crear data/ y guardar el JSON
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", encoding="utf-8") as archivo:
        json.dump(
            datos,
            archivo,
            ensure_ascii=False,
            indent=2,
        )
        archivo.write("\n")

    print("Predicción descargada correctamente.")
    print(f"Municipio: Paterna ({MUNICIPIO})")
    print(f"Archivo: {OUTPUT_FILE}")


if __name__ == "__main__":
    try:
        descargar_prediccion()
    except (requests.RequestException, RuntimeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
