import pyarrow.parquet as pq
import pytest

import historico_aire as h


@pytest.mark.parametrize("src,esp", [
    ("PM2.5", "pm2_5"), ("PM10", "pm10"), ("Dia de la semana", "dia_de_la_semana"),
    ("Dirección del viento", "direccion_del_viento"), ("_id", "ckan_id"),
    ("Humedad relativa", "humedad_relativa"), ("C6H6", "c6h6"),
])
def test_normalizar_columna(src, esp):
    assert h.normalizar_columna(src) == esp


def test_colisiones():
    assert h.normalizar_columnas(["Id", "ID", "_id"]) == ["id", "id_2", "ckan_id"]


class Resp:
    def __init__(self, d): self.d = d
    def raise_for_status(self): pass
    def json(self): return self.d


class FakeSesion:
    def __init__(self, filas, fallos=0):
        self.filas, self.fallos, self.llamadas = filas, fallos, 0
    def get(self, url, params, timeout):
        self.llamadas += 1
        if self.fallos > 0:
            self.fallos -= 1
            raise ConnectionError("caida")
        o, l = params["offset"], params["limit"]
        return Resp({"success": True, "result": {
            "total": len(self.filas),
            "fields": [{"id": "_id"}, {"id": "Fecha"}, {"id": "PM2.5"}],
            "records": self.filas[o:o + l]}})


def filas(n):
    return [{"_id": i, "Fecha": "2020-13-01" if i == 0 else "2020-01-02", "PM2.5": None if i % 2 else 7}
            for i in range(n)]


def test_descarga_texto_y_paginas(tmp_path):
    s = FakeSesion(filas(25))
    n, total = h.descargar_origen("x", "rid", tmp_path, pagina=10, sesion=s, espera=0)
    assert (n, total) == (25, 25) and s.llamadas == 3
    t = pq.read_table(tmp_path / "origen=x/part-0000.parquet")
    assert t.column_names == ["ckan_id", "fecha", "pm2_5"]
    assert all(str(f) == "string" for f in t.schema.types)
    assert t.column("fecha")[0].as_py() == "2020-13-01"  # sin arreglar
    assert t.column("pm2_5")[0].as_py() == "7" and t.column("pm2_5")[1].as_py() is None


def test_reintentos(tmp_path):
    s = FakeSesion(filas(5), fallos=2)
    assert h.descargar_origen("x", "rid", tmp_path, sesion=s, espera=0)[0] == 5


def test_reintentos_agotados(tmp_path):
    with pytest.raises(RuntimeError):
        h.descargar_origen("x", "rid", tmp_path, sesion=FakeSesion(filas(5), fallos=99), espera=0)
    assert not (tmp_path / "origen=x").exists()


def test_idempotencia_y_forzar(tmp_path):
    h.descargar_origen("x", "rid", tmp_path, sesion=FakeSesion(filas(5)), espera=0)
    s2 = FakeSesion(filas(8))
    n, total = h.descargar_origen("x", "rid", tmp_path, sesion=s2, espera=0)
    assert (n, total, s2.llamadas) == (5, None, 0)
    n, total = h.descargar_origen("x", "rid", tmp_path, forzar=True, sesion=s2, espera=0)
    assert (n, total) == (8, 8) and s2.llamadas == 1


def test_main_forzar(monkeypatch, tmp_path):
    llamadas = []
    def fake(nombre, rid, destino, forzar):
        llamadas.append(forzar); return 3, 3
    monkeypatch.setattr(h, "descargar_origen", fake)
    assert h.main(["--destino", str(tmp_path), "--forzar"]) == 0
    assert llamadas == [True, True]
