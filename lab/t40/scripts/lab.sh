#!/bin/bash
# Control del laboratorio T40. Uso: lab/t40/scripts/lab.sh <orden> [args]
#   up        construye la imagen app (si hace falta) y arranca todo; espera a que Oracle este sembrado
#   seed      crea/actualiza ConnectorConfig, ResourceConfig y ResourceSizeConfig (idempotente)
#   rebuild   reconstruye la imagen app con el src/ actual y recrea app y los Apache
#   apache MODO   MODO = none | front | back | both: recrea los Apache con mod_deflate donde se indique
#   app-env K=V...  recrea app con variables LAB_* (p. ej. LAB_GUNICORN_TIMEOUT=20 LAB_ORACLE_ARRAYSIZE=1000)
#   status    estado de los contenedores y recursos sembrados
#   logs [servicio]   docker compose logs
#   down      para el laboratorio y CONSERVA los volumenes;  down -v  tambien los borra (siembra de Oracle: varios minutos)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export COMPOSE_PROJECT_NAME=gaod-t40-lab
dc() { docker compose -p gaod-t40-lab -f "$HERE/docker-compose.yml" "$@"; }

wait_app() {
  for _ in $(seq 1 90); do
    if curl -sf -o /dev/null "http://127.0.0.1:18080/GA_OD_Core/views"; then return 0; fi
    sleep 2
  done
  echo "la cadena apache-front -> app no responde" >&2; return 1
}

cmd="${1:-status}"; shift || true
case "$cmd" in
  up)
    dc up -d --build --wait --wait-timeout 1800
    "$0" seed
    wait_app
    echo "Laboratorio listo: front http://127.0.0.1:18080  back http://127.0.0.1:18081  app http://127.0.0.1:18082"
    ;;
  seed)
    dc exec -T app bash -c "cd /opt/gaodcore && python manage.py shell" < "$HERE/seed/seed_config.py"
    ;;
  rebuild)
    dc build app
    dc up -d --force-recreate --wait app
    dc up -d --force-recreate apache-back apache-front
    wait_app
    ;;
  apache)
    mode="${1:?modo: none|front|back|both}"
    front=PLAIN; back=PLAIN
    case "$mode" in
      none) ;;
      front) front=DEFLATE ;;
      back) back=DEFLATE ;;
      both) front=DEFLATE; back=DEFLATE ;;
      *) echo "modo desconocido: $mode" >&2; exit 2 ;;
    esac
    LAB_FRONT_MODE=$front LAB_BACK_MODE=$back dc up -d --force-recreate --no-deps apache-back apache-front
    wait_app
    echo "apache deflate: $mode (front=$front back=$back)"
    ;;
  app-env)
    env "$@" bash -c "$(printf '%q ' "$0") _recreate_app"
    ;;
  _recreate_app)
    dc up -d --force-recreate --no-deps --wait app
    dc restart apache-back apache-front   # Apache resuelve el DNS de app al arrancar: la IP cambia al recrearla
    wait_app
    ;;
  status)
    dc ps
    dc exec -T app bash -c "cd /opt/gaodcore && python manage.py shell -c \"from gaodcore_manager.models import ResourceConfig as R; [print(r.id, r.name, r.object_location) for r in R.objects.order_by('id')]\"" 2>/dev/null || true
    ;;
  logs)
    dc logs "$@"
    ;;
  down)
    dc down "$@"
    ;;
  *)
    sed -n 2,13p "$0"; exit 2
    ;;
esac
