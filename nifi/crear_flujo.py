#!/usr/bin/env python3
"""Crea en Apache NiFi 2.10 el flujo de ingesta de las espiras de tráfico (capa 208).

Flujo (dentro del grupo "Espiras de tráfico (capa 208)"):

    InvokeHTTP (GET capa 208, cada hora)
      -> EvaluateJsonPath (features -> contenido)
      -> SplitJson (un flowfile por punto)
      -> UpdateAttribute (filename con marca de tiempo)
      -> PutFile (/datalake/p4_espiras = ~/nifi/datalake/p4_espiras del equipo)

    Los errores (Failure/No Retry/Retry, unmatched, failure) van a un embudo
    ("funnel") sin salida: se quedan en cola y se pueden inspeccionar en la UI.

NiFi es el contenedor que ya existía en el equipo, bigdata-nifi (proyecto ~/nifi,
ver docker-compose.yml del repo). En su lienzo raíz hay otro ejercicio (GetFile ->
QueryRecord -> UpdateAttribute -> PutFile): este script solo crea, cambia o borra
su propio grupo de proceso y nunca toca lo demás.

Todo se hace por la API REST (https://localhost:8443/nifi-api) con un token de
/access/token. Solo usa la biblioteca estándar de Python.

Uso (desde la raíz del repo, con NiFi arrancado: docker start bigdata-nifi):

    python nifi/crear_flujo.py crear              # crea el grupo y lo arranca (cada 1 hour)
    python nifi/crear_flujo.py crear --recrear    # borra el grupo si existe y lo vuelve a crear
    python nifi/crear_flujo.py periodo "1 min"    # cambia la planificación de InvokeHTTP (prueba)
    python nifi/crear_flujo.py periodo "1 hour"   # ... y la deja otra vez en 1 hora
    python nifi/crear_flujo.py una-vez            # ejecuta InvokeHTTP una sola vez (Run Once)
    python nifi/crear_flujo.py estado             # contadores de flowfiles y avisos (bulletins)
    python nifi/crear_flujo.py exportar nifi/flujo_espiras.json   # "Download flow definition"
    python nifi/crear_flujo.py parar              # detiene todos los procesadores del grupo
    python nifi/crear_flujo.py arrancar           # los vuelve a poner en marcha

Ojo: un procesador "timer driven" se ejecuta en cuanto se arranca y luego cada periodo;
y NiFi recuerda el estado: al volver a arrancar el contenedor el flujo sigue en marcha.
Por eso, antes de "docker stop bigdata-nifi", conviene "python nifi/crear_flujo.py parar".

Credenciales: variables NIFI_USERNAME / NIFI_PASSWORD o, si no están, el fichero
.env.nifi de la raíz del repo (fuera de Git). NIFI_URL cambia la dirección de la API.
"""

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

RAIZ_REPO = Path(__file__).resolve().parent.parent
FICHERO_ENV = RAIZ_REPO / ".env.nifi"

NOMBRE_GRUPO = "Espiras de tráfico (capa 208)"
# A la derecha de los procesadores del otro ejercicio (x de -840 a -232 en el lienzo raíz).
POSICION_GRUPO = {"x": 320.0, "y": -112.0}
URL_CAPA_208 = (
    "https://geoportal.valencia.es/server/rest/services/OPENDATA/Trafico/"
    "MapServer/208/query?where=1=1&outFields=*&f=geojson"
)
# Dentro del contenedor. El único montaje de bigdata-nifi es ~/nifi/datalake -> /datalake;
# p4_espiras/ es la subcarpeta de esta práctica (input/ y output/ son del otro ejercicio).
DIRECTORIO_SALIDA = "/datalake/p4_espiras"
# El contenedor está en UTC (no tiene TZ): la zona se pasa a format() para que el
# nombre lleve la hora de Valencia.
NOMBRE_FICHERO = (
    "espiras_${now():format('yyyyMMdd-HHmmss', 'Europe/Madrid')}"
    "_${fragment.index:padLeft(4, '0')}.json"
)
PERIODO_POR_DEFECTO = "1 hour"

P = "org.apache.nifi.processors."
TIPO = {
    "invoke": P + "standard.InvokeHTTP",
    "evaluate": P + "standard.EvaluateJsonPath",
    "split": P + "standard.SplitJson",
    "update": P + "attributes.UpdateAttribute",
    "put": P + "standard.PutFile",
}


# --------------------------------------------------------------------------- API


def leer_credenciales():
    valores = {}
    if FICHERO_ENV.exists():
        for linea in FICHERO_ENV.read_text(encoding="utf-8").splitlines():
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                clave, valor = linea.split("=", 1)
                valores[clave.strip()] = valor.strip().strip('"').strip("'")
    usuario = os.environ.get("NIFI_USERNAME") or valores.get("NIFI_USERNAME", "admin")
    clave = os.environ.get("NIFI_PASSWORD") or valores.get("NIFI_PASSWORD")
    if not clave:
        sys.exit(f"Falta NIFI_PASSWORD (variable de entorno o {FICHERO_ENV}).")
    return usuario, clave


class NiFi:
    def __init__(self, base, usuario, clave):
        self.base = base.rstrip("/")
        # NiFi genera un certificado autofirmado: no se verifica (equivale a curl -k).
        self.ctx = ssl._create_unverified_context()
        self.token = None
        self.token = self._esperar_token(usuario, clave)

    def _peticion(self, metodo, ruta, datos=None, form=None, crudo=False):
        cabeceras = {}
        cuerpo = None
        if form is not None:
            cuerpo = urllib.parse.urlencode(form).encode()
            cabeceras["Content-Type"] = "application/x-www-form-urlencoded"
        elif datos is not None:
            cuerpo = json.dumps(datos).encode()
            cabeceras["Content-Type"] = "application/json"
        if self.token:
            cabeceras["Authorization"] = "Bearer " + self.token
        req = urllib.request.Request(self.base + ruta, data=cuerpo, method=metodo, headers=cabeceras)
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=60) as r:
                contenido = r.read()
        except urllib.error.HTTPError as e:
            detalle = e.read().decode(errors="replace")[:800]
            raise RuntimeError(f"{metodo} {ruta} -> HTTP {e.code}: {detalle}") from None
        if crudo:
            return contenido
        return json.loads(contenido) if contenido else None

    def _esperar_token(self, usuario, clave, espera=240):
        """NiFi tarda en arrancar: reintenta hasta que /access/token responde."""
        limite = time.time() + espera
        while True:
            try:
                return self._peticion(
                    "POST", "/access/token", form={"username": usuario, "password": clave}, crudo=True
                ).decode()
            except (urllib.error.URLError, ConnectionError, RuntimeError, OSError) as e:
                if isinstance(e, RuntimeError) and ("HTTP 400" in str(e) or "HTTP 401" in str(e)):
                    sys.exit("NiFi rechaza las credenciales. Revisa .env.nifi.")
                if time.time() > limite:
                    sys.exit(f"NiFi no responde en {self.base} (¿docker start bigdata-nifi?): {e}")
                time.sleep(5)

    def get(self, ruta):
        return self._peticion("GET", ruta)

    def post(self, ruta, datos):
        return self._peticion("POST", ruta, datos)

    def put(self, ruta, datos):
        return self._peticion("PUT", ruta, datos)

    def delete(self, ruta):
        return self._peticion("DELETE", ruta)


# ----------------------------------------------------------------- utilidades


def grupo_raiz(api):
    return api.get("/flow/process-groups/root")["processGroupFlow"]["id"]


def buscar_grupo(api, raiz):
    for g in api.get(f"/process-groups/{raiz}/process-groups")["processGroups"]:
        if g["component"]["name"] == NOMBRE_GRUPO:
            return g
    return None


def grupo_o_salir(api):
    g = buscar_grupo(api, grupo_raiz(api))
    if not g:
        sys.exit(f'No existe el grupo "{NOMBRE_GRUPO}". Créalo con: python nifi/crear_flujo.py crear')
    return g


def procesadores(api, grupo_id):
    return {p["component"]["name"]: p for p in api.get(f"/process-groups/{grupo_id}/processors")["processors"]}


def cambiar_estado_grupo(api, grupo_id, estado):
    """Solo afecta a los procesadores de este grupo, no al resto del lienzo."""
    api.put(f"/flow/process-groups/{grupo_id}", {"id": grupo_id, "state": estado})


def esperar_sin_hilos(api, grupo_id, espera=60):
    limite = time.time() + espera
    while time.time() < limite:
        snap = api.get(f"/flow/process-groups/{grupo_id}/status")["processGroupStatus"]["aggregateSnapshot"]
        if snap["activeThreadCount"] == 0:
            return
        time.sleep(1)


def vaciar_colas(api, grupo_id):
    for c in api.get(f"/process-groups/{grupo_id}/connections")["connections"]:
        pet = api.post(f"/flowfile-queues/{c['id']}/drop-requests", {})["dropRequest"]
        while not pet["finished"]:
            time.sleep(0.5)
            pet = api.get(f"/flowfile-queues/{c['id']}/drop-requests/{pet['id']}")["dropRequest"]
        api.delete(f"/flowfile-queues/{c['id']}/drop-requests/{pet['id']}")


def borrar_grupo(api, g):
    gid = g["id"]
    cambiar_estado_grupo(api, gid, "STOPPED")
    esperar_sin_hilos(api, gid)
    # Por si se añadió algún servicio desde la UI: NiFi no borra un grupo con servicios habilitados.
    servicios = api.get(f"/flow/process-groups/{gid}/controller-services"
                        "?includeAncestorGroups=false")["controllerServices"]
    if servicios:
        api.put(f"/flow/process-groups/{gid}/controller-services", {"id": gid, "state": "DISABLED"})
        time.sleep(3)
    vaciar_colas(api, gid)
    version = api.get(f"/process-groups/{gid}")["revision"]["version"]
    api.delete(f"/process-groups/{gid}?version={version}")
    print(f'Borrado el grupo anterior "{NOMBRE_GRUPO}" ({gid}).')


# ------------------------------------------------------------- construcción


def crear_procesador(api, grupo_id, tipo, nombre, x, y, propiedades, dinamicas=(),
                     auto_terminar=(), periodo=None, comentario=""):
    config = {"properties": propiedades, "autoTerminatedRelationships": list(auto_terminar),
              "comments": comentario}
    if periodo:
        config.update(schedulingStrategy="TIMER_DRIVEN", schedulingPeriod=periodo)
    ent = api.post(f"/process-groups/{grupo_id}/processors", {
        "revision": {"version": 0},
        "component": {"type": tipo, "name": nombre, "position": {"x": x, "y": y}, "config": config},
    })
    # Un nombre de propiedad mal escrito NiFi lo toma como propiedad dinámica (en
    # InvokeHTTP, ¡como cabecera HTTP!): se comprueba contra los descriptores. En NiFi 2
    # cambiaron nombres de 1.x (p. ej. "Remote URL" -> "HTTP URL"), así que importa.
    descriptores = ent["component"]["config"]["descriptors"]
    for clave in propiedades:
        es_dinamica = descriptores.get(clave, {}).get("dynamic", True)
        if es_dinamica and clave not in dinamicas:
            raise RuntimeError(f"{nombre}: la propiedad '{clave}' no existe en {tipo}")
    return ent["id"]


def conectar(api, grupo_id, origen, destino, relaciones, tipo_destino="PROCESSOR"):
    api.post(f"/process-groups/{grupo_id}/connections", {
        "revision": {"version": 0},
        "component": {
            "source": {"id": origen, "groupId": grupo_id, "type": "PROCESSOR"},
            "destination": {"id": destino, "groupId": grupo_id, "type": tipo_destino},
            "selectedRelationships": relaciones,
        },
    })


def construir(api, periodo):
    raiz = grupo_raiz(api)
    grupo = api.post(f"/process-groups/{raiz}/process-groups", {
        "revision": {"version": 0},
        "component": {"name": NOMBRE_GRUPO, "position": POSICION_GRUPO,
                      "comments": "Práctica 4 BDA: ingesta de la capa 208 del Geoportal de Valencia con NiFi."},
    })["id"]

    api.post(f"/process-groups/{grupo}/labels", {
        "revision": {"version": 0},
        "component": {
            "label": ("Espiras de tráfico (Geoportal de Valencia, capa 208)\n"
                      "Cada hora: GeoJSON -> un fichero por punto en /datalake/p4_espiras\n"
                      "Los errores se quedan en la cola del embudo de la derecha."),
            "position": {"x": 0, "y": -110}, "width": 460, "height": 80,
            "style": {"font-size": "13px"},
        },
    })

    # Sin SSL Context Service: el Java 21 de la imagen 2.10 (BellSoft Liberica) ya trae la
    # raíz ACCVRAIZ1 de geoportal.valencia.es en su cacerts (con 1.28 / Java 11 no estaba).
    invoke = crear_procesador(
        api, grupo, TIPO["invoke"], "Descargar capa 208 (GET)", 0, 0,
        {"HTTP Method": "GET", "HTTP URL": URL_CAPA_208,
         "Connection Timeout": "10 secs", "Socket Read Timeout": "30 secs"},
        auto_terminar=["Original"], periodo=periodo,
        comentario="Geoportal de Valencia, Trafico/MapServer/208 en GeoJSON (~1200 puntos).")
    evaluate = crear_procesador(
        api, grupo, TIPO["evaluate"], "Extraer features", 0, 200,
        {"Destination": "flowfile-content", "Return Type": "json", "features": "$.features"},
        dinamicas={"features"},
        comentario="Sustituye el contenido por el array features del GeoJSON.")
    split = crear_procesador(
        api, grupo, TIPO["split"], "Un flowfile por punto", 0, 400,
        {"JsonPath Expression": "$[*]"},
        auto_terminar=["original"],
        comentario="Cada elemento del array (una espira) pasa a ser un flowfile.")
    update = crear_procesador(
        api, grupo, TIPO["update"], "Nombre con marca de tiempo", 0, 600,
        {"filename": NOMBRE_FICHERO},
        dinamicas={"filename"},
        comentario="fragment.index lo pone SplitJson: evita choques dentro del mismo segundo.")
    put = crear_procesador(
        api, grupo, TIPO["put"], "Guardar en /datalake/p4_espiras", 0, 800,
        {"Directory": DIRECTORIO_SALIDA, "Conflict Resolution Strategy": "fail",
         "Create Missing Directories": "true"},
        auto_terminar=["success"],
        comentario="En el equipo: ~/nifi/datalake/p4_espiras (montaje del contenedor bigdata-nifi).")

    embudo = api.post(f"/process-groups/{grupo}/funnels", {
        "revision": {"version": 0}, "component": {"position": {"x": 760, "y": 430}},
    })["id"]

    conectar(api, grupo, invoke, evaluate, ["Response"])
    conectar(api, grupo, evaluate, split, ["matched"])
    conectar(api, grupo, split, update, ["split"])
    conectar(api, grupo, update, put, ["success"])
    conectar(api, grupo, invoke, embudo, ["Failure", "No Retry", "Retry"], "FUNNEL")
    conectar(api, grupo, evaluate, embudo, ["failure", "unmatched"], "FUNNEL")
    conectar(api, grupo, split, embudo, ["failure"], "FUNNEL")
    conectar(api, grupo, put, embudo, ["failure"], "FUNNEL")

    # Todos los procesadores tienen que quedar válidos antes de arrancar.
    for _ in range(20):
        estados = {n: p["component"] for n, p in procesadores(api, grupo).items()}
        if not any(c["validationStatus"] == "VALIDATING" for c in estados.values()):
            break
        time.sleep(0.5)
    malos = [f"  {n}: {c.get('validationErrors')}" for n, c in estados.items()
             if c["validationStatus"] != "VALID"]
    if malos:
        sys.exit("Procesadores no válidos:\n" + "\n".join(malos))
    return grupo


# --------------------------------------------------------------- órdenes


def orden_crear(api, args):
    existente = buscar_grupo(api, grupo_raiz(api))
    if existente:
        if not args.recrear:
            sys.exit(f'Ya existe "{NOMBRE_GRUPO}". Usa --recrear para borrarlo y crearlo de nuevo.')
        borrar_grupo(api, existente)
    grupo = construir(api, args.periodo)
    print(f'Creado "{NOMBRE_GRUPO}" ({grupo}) con InvokeHTTP cada {args.periodo}.')
    if args.no_arrancar:
        print("Sin arrancar (--no-arrancar).")
    else:
        cambiar_estado_grupo(api, grupo, "RUNNING")
        print("Grupo arrancado. InvokeHTTP se ejecuta ya y luego según la planificación.")


def orden_periodo(api, args):
    g = grupo_o_salir(api)
    p = next(p for n, p in procesadores(api, g["id"]).items() if p["component"]["type"] == TIPO["invoke"])
    pid = p["id"]
    estaba = p["component"]["state"]
    if estaba == "RUNNING":
        api.put(f"/processors/{pid}/run-status", {"revision": p["revision"], "state": "STOPPED"})
    rev = api.get(f"/processors/{pid}")["revision"]
    api.put(f"/processors/{pid}", {"revision": rev, "component": {
        "id": pid, "config": {"schedulingStrategy": "TIMER_DRIVEN", "schedulingPeriod": args.valor}}})
    if estaba == "RUNNING":
        rev = api.get(f"/processors/{pid}")["revision"]
        api.put(f"/processors/{pid}/run-status", {"revision": rev, "state": "RUNNING"})
    print(f"InvokeHTTP: planificación cada {args.valor} (estado: {estaba}).")


def orden_una_vez(api, args):
    g = grupo_o_salir(api)
    procs = procesadores(api, g["id"])
    # El resto del flujo tiene que estar en marcha para que los datos lleguen a PutFile.
    for p in procs.values():
        if p["component"]["type"] != TIPO["invoke"] and p["component"]["state"] != "RUNNING":
            api.put(f"/processors/{p['id']}/run-status", {"revision": p["revision"], "state": "RUNNING"})
    p = next(p for p in procs.values() if p["component"]["type"] == TIPO["invoke"])
    if p["component"]["state"] == "RUNNING":
        api.put(f"/processors/{p['id']}/run-status", {"revision": p["revision"], "state": "STOPPED"})
        p = api.get(f"/processors/{p['id']}")
    api.put(f"/processors/{p['id']}/run-status", {"revision": p["revision"], "state": "RUN_ONCE"})
    print("InvokeHTTP ejecutado una vez; queda parado ('arrancar' lo devuelve a su planificación).")


def orden_estado(api, args):
    g = grupo_o_salir(api)
    st = api.get(f"/flow/process-groups/{g['id']}/status")["processGroupStatus"]["aggregateSnapshot"]
    print(f"Grupo: {st['name']}  (ventana de 5 min)  en cola: {st['queued']}  hilos activos: {st['activeThreadCount']}")
    print(f"{'Procesador':32} {'Estado':9} {'Entrada':>16} {'Salida':>16} {'Tareas':>7}")
    filas = sorted(st["processorStatusSnapshots"], key=lambda s: s["processorStatusSnapshot"]["name"])
    for s in filas:
        s = s["processorStatusSnapshot"]
        print(f"{s['name'][:32]:32} {s['runStatus'][:9]:9} {s['input']:>16} {s['output']:>16} {s['taskCount']:>7}")
    for c in st["connectionStatusSnapshots"]:
        c = c["connectionStatusSnapshot"]
        if c["flowFilesQueued"]:
            print(f"  cola {c['sourceName']} -> {c['destinationName']}: {c['queued']}")
    avisos = api.get(f"/flow/bulletin-board?groupId={g['id']}&limit=10")["bulletinBoard"]["bulletins"]
    for b in avisos:
        b = b.get("bulletin", {})
        print(f"  [{b.get('level')}] {b.get('timestamp')} {b.get('sourceName')}: {b.get('message', '')[:200]}")


def orden_exportar(api, args):
    g = grupo_o_salir(api)
    definicion = json.loads(api._peticion("GET", f"/process-groups/{g['id']}/download", crudo=True))
    destino = Path(args.ruta)
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(definicion, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Definición del flujo guardada en {destino} ({destino.stat().st_size} bytes).")


def orden_parar(api, args):
    g = grupo_o_salir(api)
    cambiar_estado_grupo(api, g["id"], "STOPPED")
    print(f'Grupo "{NOMBRE_GRUPO}" parado.')


def orden_arrancar(api, args):
    g = grupo_o_salir(api)
    cambiar_estado_grupo(api, g["id"], "RUNNING")
    print(f'Grupo "{NOMBRE_GRUPO}" en marcha.')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="orden", required=True)
    c = sub.add_parser("crear", help="crea el grupo de proceso y lo arranca")
    c.add_argument("--periodo", default=PERIODO_POR_DEFECTO, help='planificación de InvokeHTTP, p. ej. "1 min"')
    c.add_argument("--recrear", action="store_true", help="borra el grupo si ya existe")
    c.add_argument("--no-arrancar", action="store_true", help="lo deja creado pero parado")
    pp = sub.add_parser("periodo", help="cambia la planificación de InvokeHTTP")
    pp.add_argument("valor", help='p. ej. "1 min" o "1 hour"')
    sub.add_parser("una-vez", help="ejecuta InvokeHTTP una sola vez (Run Once)")
    sub.add_parser("estado", help="contadores y avisos del grupo")
    e = sub.add_parser("exportar", help="descarga la definición del flujo (.json)")
    e.add_argument("ruta")
    sub.add_parser("parar", help="detiene el grupo")
    sub.add_parser("arrancar", help="pone el grupo en marcha")
    args = ap.parse_args()

    usuario, clave = leer_credenciales()
    api = NiFi(os.environ.get("NIFI_URL", "https://localhost:8443/nifi-api"), usuario, clave)
    ordenes = {"crear": orden_crear, "periodo": orden_periodo, "una-vez": orden_una_vez,
               "estado": orden_estado, "exportar": orden_exportar, "parar": orden_parar,
               "arrancar": orden_arrancar}
    try:
        ordenes[args.orden](api, args)
    except RuntimeError as e:
        sys.exit(str(e))


if __name__ == "__main__":
    main()
