from rest_framework import viewsets

from .models import ConexionDB, ConexionAPI
from .serializers import ConexionDBSerializer, ConexionAPISerializer


class ConexionDBViewSet(viewsets.ModelViewSet):
    serializer_class = ConexionDBSerializer
    queryset = ConexionDB.objects.all()


class ConexionAPIViewSet(viewsets.ModelViewSet):
    serializer_class = ConexionAPISerializer
    queryset = ConexionAPI.objects.all()
