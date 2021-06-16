from rest_framework import serializers

from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from gaodcore_manager.validators import uri_validator, resource_validator


class ConnectorConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConnectorConfig
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']

    @staticmethod
    def validate_uri(uri: str):
        uri_validator(uri)
        return uri


class ResourceConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ResourceConfig
        fields = '__all__'
        read_only_fields = ['created_at', 'updated_at']

    def validate(self, data):
        object_location_schema = data.get('object_location_schema')
        resource_validator(uri=data['connector_config'].uri,
                           object_location=data['object_location'],
                           object_location_schema=object_location_schema)
        return data
