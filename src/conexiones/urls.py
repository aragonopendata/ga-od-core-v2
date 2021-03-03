from rest_framework import routers

from .views import ConexionDBViewSet

router = routers.SimpleRouter()
router.register(r'conexion-db', ConexionDBViewSet)
urlpatterns = router.urls
