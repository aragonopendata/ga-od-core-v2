#!/bin/bash
# Primer arranque del volumen de configdb: crea la base "labdata" con una tabla grande de PostgreSQL
# (para comparar con Oracle). lab_ready marca que la siembra ha terminado.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U postgres -d postgres -c "CREATE DATABASE labdata"
psql -v ON_ERROR_STOP=1 -U postgres -d labdata <<'SQL'
CREATE TABLE pg_big AS
SELECT g AS id,
       'name_' || g AS name,
       round((random() * 100000)::numeric, 2) AS value,
       date '2020-01-01' + (g % 2000) AS created,
       'cat_' || (g % 50) AS category,
       md5(g::text) || md5((g * 7)::text) AS note
FROM generate_series(1, 1500000) g;
ALTER TABLE pg_big ADD PRIMARY KEY (id);
ANALYZE pg_big;
CREATE TABLE lab_ready (ok int);
SQL
