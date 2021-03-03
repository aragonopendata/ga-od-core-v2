from rest_framework import routers

from .views import ConexionDBViewSet, ConexionAPIViewSet, GAViewViewSet

router = routers.SimpleRouter()
router.register(r'conexion-db', ConexionDBViewSet)
router.register(r'conexion-api', ConexionAPIViewSet)
router.register(r'ga-view', GAViewViewSet)
urlpatterns = router.urls
