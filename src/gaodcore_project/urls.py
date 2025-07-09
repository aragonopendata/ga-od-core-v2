"""gaodcore URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
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
from django.shortcuts import redirect

from .schema_views import PublicSchemaView, AdminSchemaView, PublicSwaggerView, AdminSwaggerView

urlpatterns = [
    re_path(
        r'^GA_OD_Core/',
        include([
            path('', include('gaodcore.urls')),
            path('gaodcore-transports/', include('gaodcore_transports.urls')),
            path('ui/schema/', PublicSchemaView.as_view(), name='schema'),
            path('ui/', PublicSwaggerView.as_view(url_name='schema'), name='schema-swagger-ui'),
        ])),
    re_path(
        r'^GA_OD_Core_admin/',
        include([
            path('', lambda request: redirect('/GA_OD_Core_admin/manager/views/resources/')),
            path('admin/', admin.site.urls),
            path('manager/', include('gaodcore_manager.urls')),
            path('ui/schema/', AdminSchemaView.as_view(), name='admin-schema'),
            path('ui/', AdminSwaggerView.as_view(url_name='admin-schema'), name='admin-schema-swagger-ui'),
        ]))
]
