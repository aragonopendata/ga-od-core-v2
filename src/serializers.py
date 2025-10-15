"""Module that contains all custom serializers."""

from typing import List, Dict, Any

from rest_framework import serializers
from rest_framework.fields import Field


class DictSerializer(serializers.Serializer):
    """Generic serialize to serialize a list of dict. This is normally used when resources is provided by external
    source."""

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

    def __init__(self, data: List[Dict[str, Any]], *args, **kwargs):
        self._data = data
        super().__init__(*args, **kwargs)

    def get_fields(self):
        fields = {field for row in self._data for field in row.keys()}
        return {field: Field(label=field) for field in fields}
