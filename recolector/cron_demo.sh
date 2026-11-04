#!/usr/bin/env bash
# Demostracion: NO instala nada (ni crontab -e ni timers). Muestra la crontab
# que se usaria y ejecuta UNA vez el recolector como lo haria cron.
set -u
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$RAIZ/.venv/bin/python"
REC="$RAIZ/recolector/recolector.py"
LOG="$RAIZ/logs/cron.log"
mkdir -p "$RAIZ/logs"

cat <<EOT
=== Crontab propuesta (no se instala) ===
# espiras: el conteo se refresca ~cada hora -> cada hora
0 * * * *      $PY $REC espiras >> $LOG 2>&1
# tramos: estado del trafico cambia rapido -> cada 15 min
*/15 * * * *   $PY $REC tramos >> $LOG 2>&1
# estado: idem, 15 min
*/15 * * * *   $PY $REC estado >> $LOG 2>&1
# aire: mediciones horarias -> cada hora (minuto 10)
10 * * * *     $PY $REC aire >> $LOG 2>&1
# meteo municipal: dato horario -> cada hora (minuto 20)
20 * * * *     $PY $REC meteo >> $LOG 2>&1
# aemet: prediccion diaria, se publica de madrugada -> una vez al dia 06:30
30 6 * * *     $PY $REC aemet >> $LOG 2>&1
# aemet_obs: observacion convencional, cuota de la API limitada -> cada 6 h
0 */6 * * *    $PY $REC aemet_obs >> $LOG 2>&1
EOT

echo
echo "=== Ejecucion unica simulando cron (env -i, PATH minimo) ==="
{ echo "===== cron_demo $(date -u +%Y-%m-%dT%H:%M:%SZ) ====="; } >> "$LOG"
env -i PATH=/usr/bin:/bin HOME="$HOME" "$PY" "$REC" espiras tramos estado aire meteo aemet aemet_obs >> "$LOG" 2>&1
echo "codigo de salida: $? (log en $LOG)"
