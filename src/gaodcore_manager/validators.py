from typing import Any, Dict, Iterable, Optional

from rest_framework.exceptions import ValidationError

from connectors import validate_resource, NoObjectError, DriverConnectionError, TooManyRowsError, validate_uri, \
    NotImplementedSchemaError


def uri_validator(uri):
    """Validate if uri is available.

    @param uri: URI of a database or API. This url must content basic credentials.
    """
    try:
        validate_uri(uri)
    except DriverConnectionError:
        raise ValidationError('Connection is not available.', 400)
    except NotImplementedSchemaError as err:
        raise ValidationError(str(err), 400)


def resource_validator(uri: str, object_location: str,
                       object_location_schema: Optional[str]) -> Iterable[Dict[str, Any]]:
    """Validate if resource is available.
    @return: A iterable of dictionaries. Keys of dictionaries are the name of resource columns.
    """
    try:
        return validate_resource(uri=uri,
                                 object_location=object_location,
                                 object_location_schema=object_location_schema)
    except NoObjectError:
        raise ValidationError(f'Object "{object_location}" is not available.', 400)
    except DriverConnectionError:
        raise ValidationError('Connection is not available.', 400)
    except TooManyRowsError:
        raise ValidationError('This resource have too many rows. For security reason this is not allowed.', 400)
    except NotImplementedSchemaError as err:
        raise ValidationError(str(err), 400)
