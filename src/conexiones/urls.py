from rest_framework import routers

from .views import ConexionDBViewSet, ConexionAPIViewSet

router = routers.SimpleRouter()
router.register(r'conexion-db', ConexionDBViewSet)
router.register(r'conexion-api', ConexionAPIViewSet)
urlpatterns = router.urls
