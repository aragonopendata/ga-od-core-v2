"""Siembra de ConnectorConfig / ResourceConfig / ResourceSizeConfig del laboratorio (idempotente).

Se ejecuta dentro del contenedor app: ``python manage.py shell < /lab/seed/seed_config.py`` (lo hace scripts/lab.sh seed).
Nombres de campos del ORM: ResourceConfig.connector_config (FK), ConnectorConfig.uri.
"""
import os

from gaodcore_manager.models import ConnectorConfig, ResourceConfig, ResourceSizeConfig

SERVICE = os.environ.get("LAB_ORACLE_SERVICE", "FREEPDB1")
ORACLE_URI = f"oracle://lab_data:lab_data_pw@oracle:1521/?service_name={SERVICE}"
PG_URI = "postgresql://postgres:lab_pg_pw@configdb:5432/labdata"

connectors = {
    "lab-oracle": ORACLE_URI,
    "lab-postgres": PG_URI,
}
conn_objs = {}
for name, uri in connectors.items():
    obj, _ = ConnectorConfig.objects.update_or_create(name=name, defaults={"uri": uri, "enabled": True})
    conn_objs[name] = obj

# (nombre del recurso, conector, tabla, filas aproximadas para ResourceSizeConfig)
resources = [
    ("ora_10k", "lab-oracle", "t_10k", 10_000),
    ("ora_200k", "lab-oracle", "t_200k", 200_000),
    ("ora_1500k", "lab-oracle", "t_1500k", 1_500_000),
    # Copias del recurso grande: cada una es otro recurso para T53 (max_per_resource) y otra URL (bloqueo L2), de modo
    # que R8 pueda lanzar 4 descargas grandes simultaneas sin que los limites las rechacen.
    ("ora_1500k_b", "lab-oracle", "t_1500k", 1_500_000),
    ("ora_1500k_c", "lab-oracle", "t_1500k", 1_500_000),
    ("ora_1500k_d", "lab-oracle", "t_1500k", 1_500_000),
    ("ora_200k_b", "lab-oracle", "t_200k", 200_000),
    ("ora_200k_c", "lab-oracle", "t_200k", 200_000),
    ("ora_200k_d", "lab-oracle", "t_200k", 200_000),
    ("ora_wide", "lab-oracle", "t_wide", 20_000),
    ("ora_lob", "lab-oracle", "t_lob", 5_000),
    ("pg_1500k", "lab-postgres", "pg_big", 1_500_000),
]
heavy_rows = int(os.environ.get("LAB_HEAVY_ROWS", "100000"))
for name, connector, table, rows in resources:
    rc, _ = ResourceConfig.objects.update_or_create(
        name=name,
        defaults={"connector_config": conn_objs[connector], "enabled": True, "object_location": table,
                  "object_location_schema": None},
    )
    # T53 considera grande un recurso con >= heavy_rows filas en su ultima descarga completa: se siembra ese dato.
    ResourceSizeConfig.objects.update_or_create(resource_id=rc, defaults={"registries": rows, "size": rows * 100})
    print(f"resource id={rc.id} name={name} table={table} rows={rows} heavy={rows >= heavy_rows}")
