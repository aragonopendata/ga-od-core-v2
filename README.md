# GA OD Core

GA OD Core (Gobierno de Aragón Open Data Core) is a Django application that exposes data from external databases and HTTP APIs through a public, multi-format API.

![Aragón Open Data](docs/images/aragon-open-data.svg)

## Overview

The application stores connection and resource definitions, retrieves data from the configured sources, and serves it as JSON, CSV, SCSV, XLSX, YAML or XML. It also includes private interfaces for managing those definitions and monitoring their health.

Two concepts define the system:

- **Connector**: the connection to an external database or HTTP API.
- **Resource**: a table, view, function or remote document exposed through GA OD Core. The older term *view* is retained in some public endpoints for compatibility.

## Interfaces

| Interface | Path | Access |
|---|---|---|
| Public API | `/GA_OD_Core/` | Public |
| Public Swagger UI | `/GA_OD_Core/ui/` | Public |
| Transport API | `/GA_OD_Core/gaodcore-transports/` | Public |
| Administration entry point | `/admin/GA_OD_Core_admin/` | Staff only |
| Manager web interface | `/admin/GA_OD_Core_admin/web/manager/` | Staff only |
| Health monitor | `/admin/GA_OD_Core_admin/health/` | Staff only |
| Administrative API | `/admin/GA_OD_Core_admin/manager/` | Staff only |
| Administrative Swagger UI | `/admin/GA_OD_Core_admin/ui/` | Staff only |
| Django Admin | `/admin/GA_OD_Core_admin/admin/` | Staff only |

The administration entry point redirects to the Manager web interface, which is the default interface for managing connectors and resources. Django Admin is intended for user administration and lower-level model access. The REST API and administrative Swagger UI remain available for integrations and technical use.

Private interfaces require an active staff account. Browser access uses Django sessions; the administrative API also supports Basic Authentication and must only be used over HTTPS.

## Configuration

Runtime configuration is loaded from an external YAML file selected through `CONFIG_PATH`.

| Environment variable | Purpose | Default |
|---|---|---|
| `CONFIG_PATH` | Path to the external YAML configuration | System dependent |
| `DJANGO_LOG_LEVEL` | Django and application log level | `WARNING` |
| `SQLALCHEMY_LOG_LEVEL` | SQLAlchemy engine log level | `ERROR` |
| `GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS` | Probe external sources when saving connectors and resources | `true` |

External validation must remain enabled in production. Disabling it is only intended for isolated development or test environments where external sources are unavailable. The explicit Manager validator always performs a real probe.

See `config.template.yaml` and `.env-template` for the available settings.

## Development

The project targets Python 3.10. Create or activate a compatible environment, install the requirements, and set the test configuration before running Django commands:

```bash
export CONFIG_PATH="$PWD/config-tst.yaml"
export DJANGO_SETTINGS_MODULE=gaodcore_project.test_settings
```

Run the test suite with:

```bash
pytest src --cov=.
```

Production PostgreSQL variables (`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST` and `POSTGRES_PORT`) should not be inherited by local test runs because they can interfere with testcontainers.

Code quality checks are available through the configured pre-commit hooks, Ruff, Flake8, Pylint and YAPF.

## API errors

Non-HTML API errors use the RFC 9457 problem-details format and the `application/problem+json` media type, including when the requested success format is CSV, XLSX or another tabular format. Responses include the standard `type`, `title`, `status` and `detail` members, plus the project-specific `error_code`; validation failures also include structured field errors under `errors`.

Clients should branch on `error_code` or `type`, not on the human-readable message. The `type` URIs are stable identifiers, not documents to fetch.

| Status | Typical `error_code` values |
|---|---|
| 400 | `VALIDATION_ERROR`, `RESOURCE_UNAVAILABLE`, `INVALID_FIELD`, `INVALID_FILTER`, `INVALID_SORT`, `INVALID_COLUMNS`, `INVALID_FORMAT`, `INVALID_JSON`, `INVALID_DELIMITER`, `NOT_A_NUMBER`, `REQUIRED`, `TOO_MANY_ROWS`, `MIME_TYPE_NOT_ALLOWED` |
| 429 | `DOWNLOAD_IN_PROGRESS` (the same download is already running) |
| 502 | `BAD_GATEWAY` (an external API or service failed) |
| 503 | `CONNECTION_UNAVAILABLE`, `OBJECT_UNAVAILABLE`, `QUERY_ERROR`, `SCHEMA_NOT_IMPLEMENTED`, `SERVICE_UNAVAILABLE`, `TOO_MANY_DOWNLOADS` |

A filter on a column the resource does not have is a `400 INVALID_FIELD`. An HTTP source that currently has no rows answers any filter with an empty result.

## Transport API

`/GA_OD_Core/gaodcore-transports/` exposes data from the Aragón and Zaragoza transport APIs (vehicles, lines, stops, routes, arrival times, notices, live positions…). Every endpoint accepts the `json`, `csv`, `xml`, `yaml` and `xlsx` suffixes, and responses are cached for `common_config.cache_ttl` seconds. In `zaragoza/arrival_time`, each row carries the stop code in `stop_id` and, with the same value, in `cod_parada`.

## Locked accounts

`django-axes` locks an account after repeated failed logins ("Access locked: too many login attempts"). To unlock, run one of these inside the application container:

```bash
python manage.py axes_reset                         # all lockouts and access records
python manage.py axes_reset_ip [ip ...]             # given IP addresses
python manage.py axes_reset_username [username ...] # given usernames
python manage.py axes_reset_logs [age_days]         # access logs older than the age (default 30 days)
```

## Deployment

Uncached requests may take several minutes. The application and its reverse proxy should use a sufficiently generous request timeout; the provided Gunicorn configuration uses 240 seconds.

Gunicorn is configured in `src/gunicorn.conf.py` and tuned through `GUNICORN_*` environment variables (workers, threads, timeouts, worker recycling by request count and memory); the module docstring documents each one and its default. Large CSV, SCSV, XML, JSON and GeoJSON downloads are streamed in blocks (`common_config.download_streaming` and `download_block_rows` in the YAML configuration).
