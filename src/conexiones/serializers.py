from rest_framework import serializers

from .models import ConexionDB, ConexionAPI, GAView


class ConexionDBSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConexionDB
        read_only_fields = ['id', 'conexion_type']
        exclude = ['sqla_string']


class ConexionAPISerializer(serializers.ModelSerializer):
    class Meta:
        model = ConexionAPI
        read_only_fields = ['id', 'conexion_type']
        exclude = ['url']


class GAViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = GAView
        fields = '__all__'
        read_only_fields = ['id', 'view_type', 'columns']
