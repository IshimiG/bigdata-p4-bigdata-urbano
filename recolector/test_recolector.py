import csv
from unittest.mock import MagicMock

import pyarrow.parquet as pq
import pytest
import requests

import recolector as rc


def resp(js, status=200):
    r = MagicMock()
    r.status_code = status
    r.headers = {}
    r.json.return_value = js
    r.raise_for_status.side_effect = (
        requests.HTTPError(str(status)) if status >= 400 else None)
    return r


@pytest.fixture(autouse=True)
def entorno(tmp_path, monkeypatch):
    monkeypatch.setenv("RECOLECTOR_DATA", str(tmp_path))
    monkeypatch.setenv("AEMET_API_KEY", "clave-falsa")
    monkeypatch.setattr(rc.time, "sleep", lambda s: None)
    monkeypatch.setattr(rc, "cargar_clave", lambda: "clave-falsa")
    return tmp_path


def registro(tmp):
    with open(tmp / "_registro" / "ejecuciones.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_paginacion_particionado_y_registro(entorno, monkeypatch):
    paginas = [
        resp({"features": [{"attributes": {"id": 1}, "geometry": {"x": 1, "y": 2}}],
              "exceededTransferLimit": True}),
        resp({"features": [{"attributes": {"id": 2}, "geometry": None}]}),
    ]
    llamadas = []
    monkeypatch.setattr(rc.requests, "get", lambda url, **k: (llamadas.append(k["params"]), paginas.pop(0))[1])
    assert rc.recolectar("espiras") is True
    assert [p["resultOffset"] for p in llamadas] == [0, 1]
    ficheros = list((entorno / "trafico" / "espiras").glob("fecha=*/espiras_*Z.parquet"))
    assert len(ficheros) == 1
    t = pq.read_table(ficheros[0])
    assert t.num_rows == 2 and "captura_utc" in t.column_names
    assert pq.ParquetFile(ficheros[0]).metadata.row_group(0).column(0).compression == "SNAPPY"
    reg = registro(entorno)
    assert reg[0]["fuente"] == "espiras" and reg[0]["ok"] == "true" and reg[0]["filas"] == "2"


def test_reintento_429_y_5xx(monkeypatch):
    seq = [resp({}, 429), resp({}, 503), resp({"ok": 1})]
    monkeypatch.setattr(rc.requests, "get", lambda *a, **k: seq.pop(0))
    assert rc.get_json("http://x") == {"ok": 1}


def test_reintentos_agotados(entorno, monkeypatch):
    monkeypatch.setattr(rc.requests, "get", lambda *a, **k: (_ for _ in ()).throw(requests.ConnectionError("caida")))
    assert rc.recolectar("tramos") is False
    reg = registro(entorno)
    assert reg[0]["ok"] == "false" and "caida" in reg[0]["error"]
    assert not (entorno / "trafico").exists()


def test_aemet_dos_pasos_aplanado(entorno, monkeypatch):
    pred = [{"nombre": "Valencia", "id": "46250", "elaborado": "2026-11-04T05:00:00",
             "prediccion": {"dia": [{"fecha": "2026-11-04T00:00:00",
                                     "temperatura": {"maxima": 22, "minima": 14},
                                     "probPrecipitacion": [{"value": 10, "periodo": "00-24"},
                                                           {"value": 5, "periodo": "00-12"}]}]}}]

    def get(url, **k):
        if url.startswith(rc.BASE_AEMET):
            assert k["headers"]["api_key"] == "clave-falsa"
            return resp({"estado": 200, "datos": "https://datos.example/x"})
        return resp(pred)
    monkeypatch.setattr(rc.requests, "get", get)
    assert rc.recolectar("aemet") is True
    f = next((entorno / "aemet" / "prediccion_46250").glob("fecha=*/prediccion_*.parquet"))
    fila = pq.read_table(f).to_pylist()[0]
    assert fila["temperatura_maxima"] == 22 and fila["probabilidad_precipitacion"] == 10


def test_un_fallo_no_bloquea_las_demas(entorno, monkeypatch):
    def get(url, **k):
        if "/Trafico/MapServer/188" in url:
            return resp({}, 404)
        return resp({"features": [{"attributes": {"a": 1}}]})
    monkeypatch.setattr(rc.requests, "get", get)
    codigo = rc.main(["espiras", "tramos", "estado"])
    assert codigo == 1
    reg = {r["fuente"]: r["ok"] for r in registro(entorno)}
    assert reg == {"espiras": "true", "tramos": "false", "estado": "true"}


def test_fuente_desconocida():
    assert rc.main(["nada"]) == 2
