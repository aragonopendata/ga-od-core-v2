# Configuracion YAML del laboratorio. app-entrypoint.sh la renderiza con envsubst a /etc/gaodcore/config.yaml.
# Solo contiene datos de laboratorio. Variables: ver docker-compose.yml (servicio app).
common_config:
  secret_key: lab-secret-key-not-for-production
  allowed_hosts:
    - '*'
  csrf_trusted_origins: []
  debug: false
  cache_ttl: 300
  download_timing_log: true
  # Limites T53 activos (necesitan la BD de configuracion PostgreSQL). Los recursos de >= heavy_rows filas tienen su
  # tamano sembrado en ResourceSizeConfig, asi que cuentan como grandes desde la primera descarga.
  download_limits:
    enabled: true
    heavy_rows: ${LAB_HEAVY_ROWS}
    max_per_resource: ${LAB_MAX_PER_RESOURCE}
    max_total: ${LAB_MAX_TOTAL}
    retry_after: 120
    heavy_resource_ids: []
  # Filas por viaje de red de los cursores Oracle (R4 lo varia con LAB_ORACLE_ARRAYSIZE).
  oracle_arraysize: ${LAB_ORACLE_ARRAYSIZE}
  conn_max_age: 60
  health_monitoring:
    enabled: false
  databases:
    default:
      ENGINE: django.db.backends.postgresql
      NAME: gaodcore
      USER: postgres
      PASSWORD: lab_pg_pw
      HOST: configdb
      PORT: 5432
projects:
  transport:
    aragon:
      user: lab
      password: lab
      customer_id: 0
      base_url: http://127.0.0.1:9/
      endpoints:
        vehicles: x
        drivers: x
        live_position_latest: x
        vehicle_journey_history_latest: x
        distance_travelled: x
    zaragoza:
      base_url: http://127.0.0.1:9/
      max_concurrency: 1
      endpoints:
        lines: x
        stops: x
        routes: x
        stops_route: x
        arrival_time: x
        notices: x
        origins: x
        destinations: x
        lines_ori_des: x
        times_route: x
        exp_ori_des: x
        stops_ori_des: x
        arrival_ori_des: x
        sae: x
