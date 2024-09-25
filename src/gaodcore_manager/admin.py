# Register your models here.
from django.contrib import admin

from .models import ConnectorConfig, ResourceConfig, ResourceSizeConfig


class ConnectorConfigAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'uri', 'enabled')
    list_filter = ('enabled',)
    search_fields = ('id', 'name', 'uri')


class ResourceConfigAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'name', 'connector_config', 'object_location', 'object_location_schema', 'enabled')
    list_filter = ('enabled',)
    search_fields = (
        'id', 'name', 'connector_config__name', 'object_location', 'object_location_schema')


class ResourceSizeConfigAdmin(admin.ModelAdmin):
    list_display = ('resource_id', 'registries', 'size')
    search_fields = ('registries', 'size')


admin.site.register(ConnectorConfig, ConnectorConfigAdmin)
admin.site.register(ResourceConfig, ResourceConfigAdmin)
admin.site.register(ResourceSizeConfig, ResourceSizeConfigAdmin)
