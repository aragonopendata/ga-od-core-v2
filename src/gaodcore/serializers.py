from typing import Dict, Any, List

from django.core.exceptions import ValidationError
from rest_framework import serializers
from rest_framework.fields import Field

from gaodcore import connectors
from gaodcore.connectors import NotImplementedSchemaError, DriverConnectionError, validate_resource, NoObjectError
from gaodcore.models import ConnectorConfig, ResourceConfig


class ConnectorConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConnectorConfig
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']

    @staticmethod
    def validate_uri(uri: str):
        try:
            connectors.validate_uri(uri)
        except (NotImplementedSchemaError, DriverConnectionError) as err:
            raise ValidationError(str(err), 400)
        return uri


class ResourceConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ResourceConfig
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']

    def validate(self, data):
        validate_resource(uri=data['connector_config'].uri, object_location=data['object_location'])
        return data


class DictSerializer(serializers.Serializer):
    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

    def __init__(self, data: List[Dict[str, Any]], *args, **kwargs):
        self._data = data
        super().__init__(*args, **kwargs)

    def get_fields(self):
        return {field: Field(label=field) for field in self._data[0].keys()}
