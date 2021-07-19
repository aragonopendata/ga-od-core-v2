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
    except NotImplementedSchemaError as err:
        raise ValidationError('Schema of the URI is not available.', 400) from err
    except MimeTypeError:
        raise MimeTypeError("Mimetype of content-type is not allowed. Only allowed: CSV and XLSX mimetypes.")
    except DriverConnectionError as err:
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
    except NotImplementedSchemaError as err:
        raise ValidationError('Schema of the URI is not available.', 400) from err
    except MimeTypeError as err:
        raise ValidationError("Mimetype of content-type is not allowed. Only allowed: CSV and XLSX mimetypes.") from err
    except TooManyRowsError as err:
        raise ValidationError('This resource have too many rows. For security reason this is not allowed.') from err
    except NoObjectError as err:
        raise ValidationError('Resource is not available. Table, view, function, etc... not exists.') from err
    except DriverConnectionError as err:
        raise ValidationError('Connection is not available.', 400) from err
