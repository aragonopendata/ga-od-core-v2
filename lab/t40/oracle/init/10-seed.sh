#!/bin/bash
# Siembra de Oracle (primer arranque del volumen). Corre como lab_data (APP_USER de la imagen gvenzl).
# Tablas: t_10k, t_200k, t_1500k (misma forma), t_wide (20.000 filas x 40 columnas, ~8 KB/fila), t_lob (5.000 filas con
# un CLOB de ~20 KB). lab_ready marca el final.
set -euo pipefail
SERVICE="${LAB_ORACLE_SERVICE:-FREEPDB1}"

sqlplus -s "lab_data/lab_data_pw@localhost/${SERVICE}" <<'SQL'
WHENEVER SQLERROR EXIT FAILURE
SET FEEDBACK OFF

-- Tablas "normales": 6 columnas, ~100 bytes/fila.
CREATE TABLE t_1500k NOLOGGING AS
SELECT (a.l - 1) * 1000 + b.l AS id,
       'name_' || ((a.l - 1) * 1000 + b.l) AS name,
       round(mod(((a.l - 1) * 1000 + b.l) * 7919, 100000) / 3, 2) AS value,
       date '2020-01-01' + mod((a.l - 1) * 1000 + b.l, 2000) AS created,
       'cat_' || mod((a.l - 1) * 1000 + b.l, 50) AS category,
       rpad('note ' || ((a.l - 1) * 1000 + b.l), 60, 'x') AS note
FROM (SELECT level l FROM dual CONNECT BY level <= 1500) a,
     (SELECT level l FROM dual CONNECT BY level <= 1000) b;
ALTER TABLE t_1500k ADD PRIMARY KEY (id);
CREATE TABLE t_200k NOLOGGING AS SELECT * FROM t_1500k WHERE id <= 200000;
ALTER TABLE t_200k ADD PRIMARY KEY (id);
CREATE TABLE t_10k NOLOGGING AS SELECT * FROM t_1500k WHERE id <= 10000;
ALTER TABLE t_10k ADD PRIMARY KEY (id);

-- Tabla ancha: 10 NUMBER, 5 DATE, 20 VARCHAR2(200) y 5 VARCHAR2(1000).
DECLARE
  ddl VARCHAR2(8000) := 'CREATE TABLE t_wide NOLOGGING AS SELECT level AS id';
BEGIN
  FOR i IN 1..10 LOOP ddl := ddl || ', round(level * ' || i || ' / 7, 3) AS n' || lpad(i, 2, '0'); END LOOP;
  FOR i IN 1..5  LOOP ddl := ddl || ', date ''2021-01-01'' + mod(level * ' || i || ', 900) AS d' || lpad(i, 2, '0'); END LOOP;
  FOR i IN 1..20 LOOP ddl := ddl || ', rpad(''s' || i || '_'' || level, 180, ''abcdefghij '') AS s' || lpad(i, 2, '0'); END LOOP;
  FOR i IN 1..5  LOOP ddl := ddl || ', rpad(''l' || i || '_'' || level, 900, ''klmnopqrst '') AS l' || lpad(i, 2, '0'); END LOOP;
  ddl := ddl || ' FROM dual CONNECT BY level <= 20000';
  EXECUTE IMMEDIATE ddl;
END;
/
ALTER TABLE t_wide ADD PRIMARY KEY (id);

-- Tabla con CLOB (~20 KB por fila).
CREATE TABLE t_lob (id NUMBER PRIMARY KEY, title VARCHAR2(100), body CLOB);
INSERT INTO t_lob
SELECT level, 'doc_' || level,
       to_clob(rpad('a' || level, 4000, 'lorem ipsum ')) || to_clob(rpad('b' || level, 4000, 'dolor sit ')) ||
       to_clob(rpad('c' || level, 4000, 'amet elit ')) || to_clob(rpad('d' || level, 4000, 'sed do ')) ||
       to_clob(rpad('e' || level, 4000, 'eiusmod '))
FROM dual CONNECT BY level <= 5000;
COMMIT;

BEGIN DBMS_STATS.GATHER_SCHEMA_STATS('LAB_DATA'); END;
/
CREATE TABLE lab_ready (ok NUMBER);
INSERT INTO lab_ready VALUES (1);
COMMIT;
EXIT
SQL
