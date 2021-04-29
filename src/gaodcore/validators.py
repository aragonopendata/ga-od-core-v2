from typing import Any, Dict, Iterable

from rest_framework.exceptions import ValidationError

from gaodcore.connectors import validate_resource, NoObjectError, DriverConnectionError, TooManyRowsError, validate_uri, \
    NotImplementedSchemaError


def uri_validator(uri):
    try:
        validate_uri(uri)
    except DriverConnectionError:
        raise ValidationError('Connection is not available.', 400)
    except NotImplementedSchemaError as err:
        raise ValidationError(str(err), 400)


def resource_validator(uri: str, object_location: str) -> Iterable[Dict[str, Any]]:
    try:
        return validate_resource(uri=uri, object_location=object_location)
    except NoObjectError:
        raise ValidationError(f'Object "{object_location}" is not available.', 400)
    except DriverConnectionError:
        raise ValidationError('Connection is not available.', 400)
    except TooManyRowsError:
        raise ValidationError('This resource have too many rows. For security reason this is not allowed.', 400)
    except NotImplementedSchemaError as err:
        raise ValidationError(str(err), 400)
