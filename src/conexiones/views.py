from rest_framework import viewsets

from .models import ConexionDB
from .serializers import ConexionDBSerializer


class ConexionDBViewSet(viewsets.ModelViewSet):
    serializer_class = ConexionDBSerializer
    queryset = ConexionDB.objects.all()
