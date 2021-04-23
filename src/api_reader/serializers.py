from typing import Dict, Any, List


from rest_framework import serializers
from rest_framework.fields import Field

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
