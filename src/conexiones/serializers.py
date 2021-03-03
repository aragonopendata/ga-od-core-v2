from rest_framework import serializers

from .models import ConexionDB


class ConexionDBSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConexionDB
        read_only_fields = ['id']
        exclude = ['sqla_string']
