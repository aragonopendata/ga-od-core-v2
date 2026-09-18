from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlparse

from django.conf import settings
from rest_framework.exceptions import ValidationError

from exceptions import ServiceUnavailable, ErrorCodes

from connectors import (
    validate_resource,
    NoObjectError,
    DriverConnectionError,
    TooManyRowsError,
    validate_uri,
    NotImplementedSchemaError,
    MimeTypeError,
)


def uri_validator(uri):
    """Validate if uri is available.

    @param uri: URI of a database or API. This url must content basic credentials.
    """
    try:
        validate_uri(uri)
    except NotImplementedSchemaError as err:
        raise ValidationError(
            "Schema of the URI is not available.", ErrorCodes.SCHEMA_NOT_IMPLEMENTED
        ) from err
    except MimeTypeError as err:
        raise ValidationError(
            "Mimetype of content-type is not allowed. Only allowed: JSON mimetypes.",
            ErrorCodes.MIME_TYPE_NOT_ALLOWED,
        ) from err
    except DriverConnectionError as err:
        raise ServiceUnavailable(
            "Connection is not available.", code=ErrorCodes.CONNECTION_UNAVAILABLE
        ) from err


def external_validation_enabled() -> bool:
    """Whether saving a configuration is allowed to probe the external origin."""
    return bool(getattr(settings, "GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS", True))


def resource_local_validator(
    uri: str, object_location: Optional[str], object_location_schema: Optional[str]
) -> None:
    """Validate the field constraints that depend only on the URI scheme.

    These rules never perform I/O, so they always run, also when external
    validation is disabled. Shared by the REST serializers and the manager web
    forms.
    """
    parsed = urlparse(uri or "")
    if parsed.scheme in ["postgresql"]:
        if not object_location:
            raise ValidationError("Object location is not filled.", ErrorCodes.REQUIRED)
    elif parsed.scheme == "mysql" and object_location_schema:
        raise ValidationError(
            "Object location schema is not allowed in mysql resources",
            ErrorCodes.INVALID_FIELD,
        )
    elif parsed.scheme in ["http", "https"] and (
        object_location or object_location_schema
    ):
        raise ValidationError(
            "Object location or object location schema is not allowed in http and https resources",
            ErrorCodes.INVALID_FIELD,
        )
    # Any other scheme (oracle, mssql, sqlite, ...) has no extra field constraint here.
    # Whether it is supported at all is decided by validate_resource(), which raises
    # NotImplementedSchemaError for unsupported schemes.


def uri_persistence_validator(uri: str) -> None:
    """Validate a connector URI before persisting it, honoring the external flag."""
    if external_validation_enabled():
        uri_validator(uri)


def resource_persistence_validator(
    uri: str, object_location: Optional[str], object_location_schema: Optional[str]
) -> None:
    """Validate a resource before persisting it, honoring the external flag.

    Local field constraints always run; the external probe only runs when
    GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS is enabled.
    """
    if external_validation_enabled():
        resource_validator(uri, object_location, object_location_schema)
    else:
        resource_local_validator(uri, object_location, object_location_schema)


def resource_validator(
    uri: str, object_location: str, object_location_schema: Optional[str]
) -> Iterable[Dict[str, Any]]:
    """Validate if resource is available.
    @return: A iterable of dictionaries. Keys of dictionaries are the name of resource columns.
    """
    resource_local_validator(uri, object_location, object_location_schema)

    try:
        return validate_resource(
            uri=uri,
            object_location=object_location,
            object_location_schema=object_location_schema,
        )
    except NotImplementedSchemaError as err:
        raise ValidationError(
            "Schema of the URI is not available.", ErrorCodes.SCHEMA_NOT_IMPLEMENTED
        ) from err
    except MimeTypeError as err:
        raise ValidationError(
            "Mimetype of content-type is not allowed. Only allowed: JSON mimetypes.",
            ErrorCodes.MIME_TYPE_NOT_ALLOWED,
        ) from err
    except TooManyRowsError as err:
        raise ValidationError(
            "This resource have too many rows. For security reason this is not allowed.",
            ErrorCodes.TOO_MANY_ROWS,
        ) from err
    except NoObjectError as err:
        raise ValidationError(
            "Resource is not available. Table, view, function, etc... not exists.",
            ErrorCodes.RESOURCE_UNAVAILABLE,
        ) from err
    except DriverConnectionError as err:
        raise ServiceUnavailable(
            "Connection is not available.", code=ErrorCodes.CONNECTION_UNAVAILABLE
        ) from err
