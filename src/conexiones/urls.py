from rest_framework import routers

from .views import ConexionDBViewSet, ConexionAPIViewSet, GAViewViewSet, ListGAViewViewSet

router = routers.SimpleRouter()
router.register(r'conexion-db', ConexionDBViewSet)
router.register(r'conexion-api', ConexionAPIViewSet)
router.register(r'ga-view', GAViewViewSet)
router.register(r'list-ga-view', ListGAViewViewSet)
urlpatterns = router.urls
