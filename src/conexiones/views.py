from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated

from .models import ConexionDB, ConexionAPI, GAView
from .serializers import ConexionDBSerializer, ConexionAPISerializer, GAViewSerializer


class ConexionDBViewSet(viewsets.ModelViewSet):
    serializer_class = ConexionDBSerializer
    queryset = ConexionDB.objects.all()
    permission_classes = [IsAuthenticated]


class ConexionAPIViewSet(viewsets.ModelViewSet):
    serializer_class = ConexionAPISerializer
    queryset = ConexionAPI.objects.all()
    permission_classes = [IsAuthenticated]


class GAViewViewSet(viewsets.ModelViewSet):
    serializer_class = GAViewSerializer
    queryset = GAView.objects.all()
    permission_classes = [IsAuthenticated]


class ListGAViewViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = GAViewSerializer
    queryset = GAView.objects.all()
