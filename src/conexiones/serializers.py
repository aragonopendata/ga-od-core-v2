from rest_framework import serializers

from .models import ConexionDB, ConexionAPI


class ConexionDBSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConexionDB
        read_only_fields = ['id']
        exclude = ['sqla_string']


class ConexionAPISerializer(serializers.ModelSerializer):
    class Meta:
        model = ConexionAPI
        read_only_fields = ['id']
        exclude = ['url']
