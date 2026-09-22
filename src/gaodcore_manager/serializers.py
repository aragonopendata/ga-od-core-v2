from rest_framework import serializers

from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from gaodcore_manager.validators import (
    resource_persistence_validator,
    uri_persistence_validator,
)


class ConnectorConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConnectorConfig
        fields = "__all__"
        read_only_fields = ["id", "created_at", "updated_at"]
        extra_kwargs = {"uri": {"write_only": True}}

    def validate_uri(self, uri: str):
        # On update, only a changed URI justifies an external probe. Metadata only
        # changes such as `name` or `enabled` must not perform external I/O.
        if self.instance is not None and self.instance.uri == uri:
            return uri
        uri_persistence_validator(uri)
        return uri


class ValidatorRequestSerializer(serializers.Serializer):
    """Validates the body of a POST to ValidatorView.

    Kept separate from ``ValidatorView.get_serializer`` (which builds a
    ``DictSerializer`` from the response for rendering) - this one only
    validates the incoming request body.
    """

    uri = serializers.CharField(required=True)
    object_location = serializers.CharField(required=False, allow_blank=True)
    object_location_schema = serializers.CharField(required=False, allow_blank=True)


class ResourceConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ResourceConfig
        fields = "__all__"
        read_only_fields = ["created_at", "updated_at"]

    def validate(self, attrs):
        instance = self.instance

        def resolved(field):
            if field in attrs:
                return attrs[field]
            return getattr(instance, field, None)

        connector_config = resolved("connector_config")
        object_location = resolved("object_location")
        object_location_schema = resolved("object_location_schema")

        if instance is not None and not self._connection_fields_changed(
            instance, connector_config, object_location, object_location_schema
        ):
            # Metadata only update: nothing that defines the connection changed.
            return attrs

        resource_persistence_validator(
            uri=connector_config.uri,
            object_location=object_location,
            object_location_schema=object_location_schema,
        )
        return attrs

    @staticmethod
    def _connection_fields_changed(
        instance, connector_config, object_location, object_location_schema
    ) -> bool:
        return (
            connector_config != instance.connector_config
            or object_location != instance.object_location
            or object_location_schema != instance.object_location_schema
        )
