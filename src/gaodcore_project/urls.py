"""gaodcore URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path, re_path, include
from django.contrib import admin

from drf_yasg import openapi
from drf_yasg.views import get_schema_view
from .configswagger import HttpsSchemaGenerator

SchemaView = get_schema_view(
    openapi.Info(
        title='GA OD Core API',
        default_version='v2',
        description='GA OD Core API Swagger',
        terms_of_service='https://opendata.aragon.es/informacion/terminos-de-uso-licencias',
        contact=openapi.Contact(email='opendata@aragon.es'),
        license=openapi.License(name='EUPL License'),
    ),
    public=False,
    generator_class=HttpsSchemaGenerator,
)

urlpatterns = [
    re_path(
        r'^GA_OD_Core/',
        include([
            path('', include('gaodcore.urls')),
            path('gaodcore-transports/', include('gaodcore_transports.urls')),
            path('google-analytics/', include('google_analytics.urls')),
            re_path(r'^ui(?P<format>\.json|\.yaml)$', SchemaView.without_ui(cache_timeout=0), name='schema-json'),
            re_path(r'^ui/$', SchemaView.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
        ])),
    re_path(
        r'^GA_OD_Core_admin/',
        include([
            path('admin/', admin.site.urls),
            path('manager/', include('gaodcore_manager.urls')),
            re_path(r'^ui(?P<format>\.json|\.yaml)$', SchemaView.without_ui(cache_timeout=0), name='schema-json'),
            re_path(r'^ui/$', SchemaView.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
        ]))
]
