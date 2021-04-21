from rest_framework.exceptions import ValidationError

from gaodcore.connectors import validate_resource, NoObjectError, DriverConnectionError


def resource_validator(uri: str, object_location: str):
    try:
        return validate_resource(uri=uri, object_location=object_location)
    except (NoObjectError, DriverConnectionError) as err:
        raise ValidationError(str(err))
