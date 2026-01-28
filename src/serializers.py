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
        """Extract field names safely from mixed data types.

        Handles ErrorDetail objects and other non-dictionary data gracefully,
        returning fields only from valid dictionary-like objects.
        """
        fields = set()
        if not self._data:
            return {}
        for row in self._data:
            # Only process objects that have keys() method and are callable
            if hasattr(row, 'keys') and callable(getattr(row, 'keys')):
                try:
                    fields.update(row.keys())
                except Exception:
                    # Skip problematic rows silently
                    continue
        return {field: Field(label=field) for field in fields}
