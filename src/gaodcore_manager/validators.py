from typing import Any, Dict, Iterable, Optional

from rest_framework.exceptions import ValidationError

from connectors import validate_resource, NoObjectError, DriverConnectionError, TooManyRowsError, validate_uri, \
    NotImplementedSchemaError, MimeTypeError


def uri_validator(uri):
    """Validate if uri is available.

    @param uri: URI of a database or API. This url must content basic credentials.
    """
    try:
        validate_uri(uri)
    except (DriverConnectionError, NotImplementedSchemaError, MimeTypeError) as err:
        raise ValidationError('Connection is not available.', 400) from err


def resource_validator(uri: str, object_location: str,
                       object_location_schema: Optional[str]) -> Iterable[Dict[str, Any]]:
    """Validate if resource is available.
    @return: A iterable of dictionaries. Keys of dictionaries are the name of resource columns.
    """
    try:
        return validate_resource(uri=uri,
                                 object_location=object_location,
                                 object_location_schema=object_location_schema)
    except (DriverConnectionError, NotImplementedSchemaError, MimeTypeError, NoObjectError, TooManyRowsError) as err:
        raise ValidationError('Resource is not available.', 400) from err
