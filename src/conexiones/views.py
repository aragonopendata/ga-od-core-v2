from rest_framework import viewsets

from .models import ConexionDB, ConexionAPI, GAView
from .serializers import ConexionDBSerializer, ConexionAPISerializer, GAViewSerializer


class ConexionDBViewSet(viewsets.ModelViewSet):
    serializer_class = ConexionDBSerializer
    queryset = ConexionDB.objects.all()


class ConexionAPIViewSet(viewsets.ModelViewSet):
    serializer_class = ConexionAPISerializer
    queryset = ConexionAPI.objects.all()


class GAViewViewSet(viewsets.ModelViewSet):
    serializer_class = GAViewSerializer
    queryset = GAView.objects.all()
