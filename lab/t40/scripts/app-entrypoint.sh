#!/bin/bash
# Entrypoint del servicio app: renderiza la config, migra, crea el superusuario y arranca Gunicorn como en produccion
# (gunicorn.conf.py del repo; --timeout viene de GUNICORN_TIMEOUT, 240 por defecto).
set -euo pipefail
mkdir -p /etc/gaodcore
envsubst < /lab/config/lab-config.yaml.tpl > /etc/gaodcore/config.yaml
cd /opt/gaodcore
python manage.py migrate --noinput
python manage.py collectstatic --noinput > /dev/null
python manage.py createcachetable
python manage.py createsuperuser --noinput || true   # ya existe en arranques posteriores
echo "lab: arrancando gunicorn (workers=${GUNICORN_WORKERS} timeout=${GUNICORN_TIMEOUT} arraysize=${LAB_ORACLE_ARRAYSIZE})"
exec gunicorn gaodcore_project.wsgi -c gunicorn.conf.py --bind :6001 --access-logfile - --log-level info
