# Ampliación con Apache NiFi: ingesta de las espiras de tráfico (capa 208)

Notas de la prueba hecha el 4/11/2026 en el portátil (Arch Linux, Docker 29.9 y Compose 5.6, 16 núcleos, 15 GiB de RAM).
Sirven para rellenar la tabla comparativa Python frente a NiFi.

**NiFi reutilizado.** No se ha creado ningún contenedor para la práctica. Se usa el NiFi que ya había en el equipo para otro ejercicio:

- Contenedor `bigdata-nifi`, con la imagen `apache/nifi:2.10.0`.
- Proyecto en `~/nifi` y volumen `nifi_nifi-data`.
- HTTPS en el puerto 8443 y usuario `admin`.

En su lienzo raíz sigue el otro ejercicio (GetFile → QueryRecord → UpdateAttribute → PutFile, parado, con dos servicios CSVReader y JsonRecordSetWriter). No se ha tocado.
La práctica solo añade su propio grupo de proceso, «Espiras de tráfico (capa 208)», a la derecha de esos procesadores. Escribe en `~/nifi/datalake/p4_espiras`.

Un primer intento montó un contenedor aparte con NiFi 1.28.1 y se descartó. Algunas cifras de ese intento se citan abajo y están marcadas como «1.28».

## Ficheros

| Fichero | Para qué |
|---|---|
| `docker-compose.yml` | **Documenta** el servicio `nifi` tal como está en `~/nifi/docker-compose.yml`, pero sin la contraseña en claro (usa `.env.nifi`) y con el volumen `nifi_nifi-data` como `external`. No hay que usarlo con `up` porque chocaría con el contenedor existente |
| `.env.nifi` | `NIFI_USERNAME` y `NIFI_PASSWORD` de NiFi. Está fuera de Git (`.gitignore`) |
| `nifi/crear_flujo.py` | Crea el grupo por la API REST, lo arranca y lo para, cambia la planificación, enseña los contadores y exporta la definición. Solo usa la biblioteca estándar y nunca toca lo que hay fuera de su grupo |
| `nifi/flujo_espiras.json` | Definición del grupo exportada («Download flow definition»). No lleva ninguna propiedad sensible |
| `docs/nifi/nifi_flujo.png` | Captura del grupo en marcha con los contadores de 6 descargas |
| `docs/nifi/nifi_provenance.png` | Captura de Data Provenance de un flowfile, con el detalle del evento SEND |

Los ficheros que escribe NiFi quedan en `~/nifi/datalake/p4_espiras/` (dentro del contenedor, `/datalake/p4_espiras`). Se copian a mano a `data/raw/nifi/` del repo, que está fuera de Git.

## Pasos para reproducirlo

1. `.env.nifi` en la raíz del repo, con la contraseña del NiFi existente:

   ```
   NIFI_USERNAME=admin
   NIFI_PASSWORD=<la del contenedor bigdata-nifi>
   ```

2. Carpeta de salida: `mkdir -p ~/nifi/datalake/p4_espiras`. El usuario `nifi` del contenedor es el uid 1000, el mismo que el usuario del portátil, así que con crearla como usuario normal basta. Se comprobó con `docker exec bigdata-nifi id` y un `touch` dentro del contenedor.
3. Arrancar NiFi con `docker start bigdata-nifi`. NiFi registra «Started Application in 25.571 seconds» y la API contesta unos 30 s después del `start`. La UI está en https://localhost:8443/nifi, con un certificado autofirmado.
4. Crear el flujo, primero con una planificación corta para probarlo:

   ```bash
   python nifi/crear_flujo.py crear --periodo "1 min"   # crea el grupo y lo arranca
   python nifi/crear_flujo.py estado                    # contadores, colas y avisos
   python nifi/crear_flujo.py periodo "1 hour"          # planificación definitiva
   python nifi/crear_flujo.py exportar nifi/flujo_espiras.json
   cp -n ~/nifi/datalake/p4_espiras/espiras_*.json data/raw/nifi/   # copia local, sin sobrescribir
   ```

   El script tiene más órdenes: `crear --recrear`, `una-vez` (Run Once), `parar` y `arrancar`.
5. Antes de apagar, `python nifi/crear_flujo.py parar` y después `docker stop bigdata-nifi`. NiFi recuerda el estado de cada procesador: si el grupo se queda en marcha, en el siguiente `docker start` vuelve a descargar en el momento.

También se puede montar a mano desde la UI: arrastrar un *Process Group* al lienzo, pulsar el icono de subir fichero del diálogo y elegir `nifi/flujo_espiras.json`. Después solo queda arrancar el grupo, porque el flujo no tiene servicios ni contraseñas que rellenar.

## El flujo

Todo está en el grupo **«Espiras de tráfico (capa 208)»**:

| # | Procesador (NiFi 2.10.0) | Configuración |
|---|---|---|
| 1 | InvokeHTTP «Descargar capa 208 (GET)» | `HTTP URL` = `https://geoportal.valencia.es/server/rest/services/OPENDATA/Trafico/MapServer/208/query?where=1=1&outFields=*&f=geojson`. *Timer driven* cada `1 hour`. Sin *SSL Context Service* (ver «Depuración»). `Response` va al paso 2 y `Original` se descarta |
| 2 | EvaluateJsonPath «Extraer features» | `Destination = flowfile-content`, `Return Type = json`, `features = $.features`. El contenido pasa a ser el array de puntos |
| 3 | SplitJson «Un flowfile por punto» | `$[*]`, que da 1211 flowfiles por descarga |
| 4 | UpdateAttribute «Nombre con marca de tiempo» | `filename = espiras_${now():format('yyyyMMdd-HHmmss', 'Europe/Madrid')}_${fragment.index:padLeft(4, '0')}.json` |
| 5 | PutFile «Guardar en /datalake/p4_espiras» | `Directory = /datalake/p4_espiras`, `Conflict Resolution Strategy = fail` |

- PutFile no tiene una propiedad para el nombre del fichero: usa el atributo `filename`. Por eso hace falta el UpdateAttribute del paso 4.
- Con la marca de tiempo sola, los 1211 puntos de una descarga chocarían, porque caen en el mismo segundo. Por eso el nombre lleva también `fragment.index`, que lo pone SplitJson.
- Los errores van a un embudo sin salida y se quedan en cola para poder inspeccionarlos. Son `Failure`, `No Retry` y `Retry` de InvokeHTTP, `failure` y `unmatched` de EvaluateJsonPath, y `failure` de SplitJson y de PutFile.
- El contenedor está en UTC: su compose no define `TZ` y no se puede cambiar sin recrearlo. Por eso la zona va como segundo argumento de `format()`. En Provenance, el evento de las 16:31:16 UTC escribió `espiras_20261104-173116_0720.json`.

## Resultado de la ejecución real

- La creación por la API salió **válida a la primera**, con los 5 procesadores VALID y sin ninguna depuración. Antes de escribir el script se consultaron las propiedades de cada procesador en 2.10 (`GET /nifi-api/flow/processor-definition/...`).
- Hubo 6 descargas, según la marca de tiempo de los ficheros:
  - 17:30:16, 17:31:16 y 17:32:16, con la planificación de prueba a 1 min.
  - 17:32:25, al volver a `1 hour`, porque un procesador *timer driven* se ejecuta en cuanto se arranca.
  - 17:32:32, con `una-vez` (Run Once).
  - 17:32:51, con `arrancar`.
- **6 descargas × 1211 puntos = 7266 ficheros** en `~/nifi/datalake/p4_espiras/`, sin perder ninguno. Los 7266 se leen como `Feature` GeoJSON con geometría `Point`, de unos 423 B cada uno. En total son 3,07 MB de datos, que ocupan 29 MB en disco.
- Contadores de NiFi (ventana de 5 min, en la captura):

  | Procesador | Entrada | Salida |
  |---|---|---|
  | InvokeHTTP | — | 6 (2,95 MB) |
  | EvaluateJsonPath | 6 | 6 |
  | SplitJson | 6 | 7266 |
  | UpdateAttribute | 7266 | 7266 |
  | PutFile | 7266 | — |

  Ninguna cola tenía nada, no había avisos y `nifi-app.log` no tenía ningún ERROR.
- Se copiaron con `cp -n` a `data/raw/nifi/` del repo. Ya había 10 899 ficheros del intento con 1.28 y los nombres no chocaban, así que ahora hay 18 165. `diff -rq` no da diferencias entre origen y copia.
- La configuración final es InvokeHTTP cada 1 hora, con el grupo parado (`parar`) y el contenedor parado.
- `crear --recrear` (borrar y crear) se probó en 2.10 con un grupo de prueba de otro nombre, que se borró después. El grupo real y el otro ejercicio no se tocaron.

## Cronología (4/11/2026, hora local)

| Hora | Paso |
|---|---|
| 17:26:42 | `docker start bigdata-nifi`. NiFi arranca en 25,6 s y la API contesta hacia las 17:27:12 |
| 17:27–17:30 | Revisión por la API de lo que ya había en el lienzo (el otro ejercicio). Carpeta `p4_espiras` y permisos. Prueba de TLS con el Java del contenedor. Propiedades de los procesadores en 2.10. Adaptación del script |
| 17:30:12 | `crear --periodo "1 min"`. Grupo válido y primeros ficheros a las 17:30:16 |
| 17:30:29–17:31:57 | 45 muestras de `docker stats` |
| 17:32:24–17:32:50 | `periodo "1 hour"`, `una-vez` y `arrancar` |
| 17:33–17:37 | Capturas con Chromium sin ventana |
| 17:37:13 | `exportar` |
| 17:37:44 | `parar` y copia de los ficheros al repo |
| 17:37:51 | `docker stop bigdata-nifi`, que tarda 2 s |

Desde el `docker start` hasta los primeros ficheros pasaron unos 3,5 min, reutilizando el script del intento con 1.28.

## Depuración: lo que pasó de verdad y cómo se vio

1. **Propiedades renombradas de 1.x a 2.x.** En InvokeHTTP, `Remote URL` pasa a ser `HTTP URL` y `Read Timeout` pasa a ser `Socket Read Timeout`. Las relaciones son las mismas (`Original`, `Response`, `Retry`, `No Retry`, `Failure`).
   Si se escribe mal el nombre de una propiedad, NiFi no da error: la toma como propiedad *dinámica*, y en InvokeHTTP eso la convierte en una cabecera HTTP. Con los nombres de 1.28, `Remote URL` y `Read Timeout` se habrían colado como cabeceras y el procesador habría quedado no válido por falta de `HTTP URL`.
   Para evitarlo, `crear_flujo.py` compara cada propiedad con los descriptores del procesador y se para si no existe. Esta vez no hizo falta, porque se miraron antes las definiciones.
2. **TLS con geoportal.valencia.es: en 2.10 ya no falla.** El certificado de geoportal.valencia.es lo firma la CA raíz `ACCVRAIZ1`.
   - Con 1.28 (Java 11) InvokeHTTP fallaba con «PKIX path building failed». Hubo que crear un truststore con las CA de Java más `ACCVRAIZ1` y añadir un `StandardSSLContextService`.
   - La imagen 2.10 trae el JDK 21.0.11 de BellSoft. Su `cacerts` (161 CA) ya incluye esa raíz como `moz_accvraiz1 [jdk]`, con la misma huella SHA-256 `9A:6E:C0:12:…:41:13`. Se comprobó con `keytool -list -cacerts`.
   - También se probó con un `HttpClient` de Java dentro del contenedor, que dio HTTP 200 y 515 126 caracteres por TLSv1.2. Después se confirmó con el flujo real.
   - Por eso InvokeHTTP va **sin SSL Context Service** y ya no hay truststore, contraseña `changeit` ni `nifi/certs/`. Así tampoco se escribe nada de más en el volumen `conf` del otro proyecto.

   Si un Java futuro no trajera la raíz, el arreglo sería crear el truststore en el volumen `conf`, que es persistente, sin tocar el contenedor:

   ```bash
   docker cp ACCVRAIZ1.pem bigdata-nifi:/tmp/accv.pem   # http://www.accv.es/fileadmin/Archivos/certificados/raizaccv1.crt (DER: pasar a PEM con openssl x509 -inform der)
   docker exec bigdata-nifi bash -c 'cd conf && keytool -importkeystore -noprompt -srckeystore "$JAVA_HOME/lib/security/cacerts" -srcstorepass changeit -destkeystore truststore-salida.jks -deststoretype JKS -deststorepass changeit && keytool -importcert -noprompt -alias accvraiz1 -file /tmp/accv.pem -keystore truststore-salida.jks -storepass changeit'
   ```

   Después se añade al grupo un `StandardSSLContextService` con `Truststore Filename = /opt/nifi/nifi-current/conf/truststore-salida.jks`, `Truststore Type = JKS` y `Truststore Password = changeit`, y se pone en InvokeHTTP. Esa contraseña es una propiedad sensible y no se exporta.
3. **Reloj en UTC.** No se puede añadir `TZ` sin recrear el contenedor, así que la zona va en la expresión: `format('yyyyMMdd-HHmmss', 'Europe/Madrid')`.
4. **La marca de tiempo partida.** `now()` se evalúa en cada flowfile. En 1 de las 6 descargas los 1211 ficheros quedaron repartidos entre dos segundos: 933 con `…173225` y 278 con `…173226`.
   Si se quiere una sola marca por descarga, vale `${lineageStartDate:format(...)}`, que es el momento en que InvokeHTTP creó el flowfile. Se ha dejado `now()` porque es lo que pide el enunciado.
5. **Lienzo compartido.** El `flow.json.gz` del volumen guarda en un solo fichero el lienzo entero, con los dos ejercicios. Por eso el script nunca actúa sobre la raíz:
   - busca su grupo por el nombre;
   - `parar` y `arrancar` usan `PUT /flow/process-groups/{id-del-grupo}`;
   - el grupo se coloca en x = 320, a la derecha de los procesadores del otro ejercicio.

   En la barra superior de la UI se ven 5 componentes en marcha (los de la práctica) y 4 parados (los del otro ejercicio).
6. **Un fallo en el procesador fuente no deja flowfile.** Esto se observó con 1.28 y no se ha repetido en 2.10, porque no hubo errores. InvokeHTTP no tiene entrada, así que si falla la conexión no llega nada al embudo de errores. Solo queda el *bulletin*, que dura 5 min, y `logs/nifi-app.log`.

## Datos para la tabla comparativa Python vs NiFi

El «equivalente Python» es un script de unas 15 líneas con `urllib`, que se usó solo para medir y no está en el repo. Hace lo mismo que el flujo: descargar la capa 208 y escribir 1211 ficheros.

| Criterio | Python (script) | NiFi 2.10 (este flujo, en el contenedor existente) |
|---|---|---|
| **Tiempo de desarrollo** (medido, hecho por un asistente con la API REST) | Unos 2 min para escribirlo y funcionó a la primera | Primera vez (1.28): unos 12 min desde cero hasta los primeros ficheros, de ellos unos 5 min depurando el TLS. Adaptarlo a 2.10 reutilizando el script: unos 3,5 min desde `docker start` hasta los primeros ficheros, sin depuración. A eso se suman unos 4 min para las capturas. Hacerlo a mano en la UI la primera vez lleva más, porque hay que conocer las propiedades y relaciones de cada procesador. Esto último es una estimación, no una medida |
| **Tamaño del «código»** | ~15 líneas | 5 procesadores, 8 conexiones, 1 embudo y ningún servicio. El script que lo crea tiene ~450 líneas (con utilidades y comentarios) y la definición exportada 996 líneas JSON (38 KB) |
| **Depuración** | La traza del error en consola, se reproduce al momento y se puede usar un depurador | Un *bulletin* por procesador en segundos, colas que se pueden inspeccionar (contenido y atributos de cada flowfile), un embudo de errores y «replay» desde Provenance. En contra: los nombres de propiedades cambian entre versiones y un nombre mal escrito no da error, y un fallo en el procesador fuente no deja flowfile |
| **Visibilidad** | Lo que el script registre en el log | Muy alta sin escribir nada: contadores In/Out/Read/Write/Tasks por procesador (ventana de 5 min), colas y *Data Provenance* por flowfile. Ejemplo: el flowfile `e8d87478-c30a-4db1-998f-90734d08a01f` pasa por FORK (SplitJson, 16:31:16.078 UTC) → ATTRIBUTES_MODIFIED (UpdateAttribute, .251) → SEND (PutFile, .619, `file:/datalake/p4_espiras/espiras_20261104-173116_0720.json`) → DROP (.620). Su linaje dura 0,757 s |
| **Versionado en Git** | Directo: un fichero `.py` con diffs pequeños y legibles | El flujo vive en el contenedor (`conf/flow.json.gz`), mezclado con el otro ejercicio, así que el volumen no sirve para versionar. Para Git hay dos opciones. Una es `crear_flujo.py`, que es reproducible y se puede comparar con diffs. La otra es el JSON exportado, que es largo y cambia si se mueve una caja o cambian los identificadores. Lo serio sería un *Flow Registry Client* (NiFi Registry o un repositorio Git), que no se ha montado |
| **Planificación** | Hace falta cron, un temporizador de systemd o un bucle | Integrada: *Timer driven* `1 hour` o CRON. Hay que tener en cuenta que, al arrancar el procesador, se ejecuta en el momento, y que NiFi reanuda solo lo que estaba en marcha |
| **RAM** | Máximo 27 MB por ejecución y 0 entre ejecuciones | **1,37 GiB** recién arrancado, antes de crear el flujo. **1,44–1,46 GiB** con el flujo en marcha, unos 80–95 MB más. 1,72 GiB después de usar la UI y Provenance. El heap está fijado a 1 GB (`-Xms1g -Xmx1g` en `bootstrap.conf`) |
| **CPU** | 0,07 s de CPU y 0,79 s de reloj por descarga | En reposo, del 2,3 al 3,8 % (picos sueltos de hasta el 7,9 %). En cada descarga, un pico de **342 %** en una muestra de ~2 s. Sumando el *Tasks/Time* de los 5 procesadores salen ~1,5 s por descarga, de ellos 0,56 s del GET. Hay 121–135 procesos e hilos |
| **Disco y arranque** | Python ya instalado | Imagen de 1,30 GB, más 905 MB en la capa del contenedor. Arranca en 25,6 s y la API contesta a los ~30 s. Se para en 2 s |

El contenedor ya existía, así que añadir el flujo no costó otra JVM: el coste marginal fue de unos 80–95 MB de RAM. Si NiFi solo se usara para esto, habría que contar el contenedor entero.

Lectura de las mediciones de `docker stats --no-stream bigdata-nifi`:

- Recién arrancado, sin el flujo (17:30:10): `1.89% 1.367GiB / 15.24GiB 8.97% PIDS 121`.
- 45 muestras seguidas con el flujo a 1 min (17:30:29–17:31:57): la CPU entre el 2,3 y el 7,9 %, con un pico de 342,07 % a las 17:31:14 durante la descarga; la memoria entre 1,444 y 1,460 GiB y 127–131 PIDs.
- Al final, después de usar la UI y Provenance (17:37:13): `2.36% 1.718GiB / 15.24GiB 11.28% NET 4.35MB / 26.6MB BLOCK 2.18GB / 88.3MB PIDS 135`.

## Capturas

Las capturas se hicieron con Chromium sin ventana (headless), controlado con `playwright-core` desde una carpeta temporal, sin tocar el escritorio. Se inició sesión como admin.

- `docs/nifi/nifi_flujo.png` (1600×1240) es el grupo en marcha a las 17:34. Muestra los 5 procesadores ▶, los contadores de las 6 descargas y las colas vacías. En el panel *Operation* aparece el id del grupo, y la barra superior muestra 5 componentes en marcha y 4 parados (los del otro ejercicio).
- `docs/nifi/nifi_provenance.png` (1600×1180) es Data Provenance buscado por FlowFile UUID (`e8d87478-…`), con sus 4 eventos. Encima va el detalle del evento SEND de PutFile. Al *Transit Uri* se le quitó el recorte con puntos suspensivos para que se lea entera, sin cambiar su contenido.
