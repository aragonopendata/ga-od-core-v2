from django.utils.decorators import method_decorator
from drf_renderer_xlsx.mixins import XLSXFileMixin
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from gaodcore_manager.serializers import (
    ConnectorConfigSerializer,
    ResourceConfigSerializer,
)
from gaodcore_manager.validators import resource_validator
from utils import get_return_list
from views import APIViewMixin


@method_decorator(name="create", decorator=extend_schema(tags=["manager"]))
@method_decorator(name="update", decorator=extend_schema(tags=["manager"]))
@method_decorator(
    name="partial_update", decorator=extend_schema(tags=["manager"])
)
@method_decorator(name="destroy", decorator=extend_schema(tags=["manager"]))
@method_decorator(name="list", decorator=extend_schema(tags=["manager"]))
@method_decorator(name="retrieve", decorator=extend_schema(tags=["manager"]))
class ConnectorConfigView(XLSXFileMixin, viewsets.ModelViewSet):
    serializer_class = ConnectorConfigSerializer
    queryset = ConnectorConfig.objects.all()
    permission_classes = (IsAuthenticated,)


@method_decorator(name="create", decorator=extend_schema(tags=["manager"]))
@method_decorator(name="update", decorator=extend_schema(tags=["manager"]))
@method_decorator(
    name="partial_update", decorator=extend_schema(tags=["manager"])
)
@method_decorator(name="destroy", decorator=extend_schema(tags=["manager"]))
@method_decorator(name="list", decorator=extend_schema(tags=["manager"]))
@method_decorator(name="retrieve", decorator=extend_schema(tags=["manager"]))
class ResourceConfigView(XLSXFileMixin, viewsets.ModelViewSet):
    serializer_class = ResourceConfigSerializer
    queryset = ResourceConfig.objects.all()
    permission_classes = (IsAuthenticated,)


class ValidatorView(APIViewMixin):
    permission_classes = (IsAuthenticated,)

    @staticmethod
    @extend_schema(
        tags=["manager"],
        parameters=[
            OpenApiParameter(
                "uri",
                required=True,
                description="URI of resource. Not allowed driver in schema.",
                type=OpenApiTypes.STR,
            ),
            OpenApiParameter(
                "object_location",
                required=False,
                description="This field in databases origins can be a table, view or function. "
                "This field in APIs origins is not required.",
                type=OpenApiTypes.STR,
            ),
            OpenApiParameter(
                "object_location_schema",
                required=False,
                description="Schema of object_location. Normally used in databases",
                type=OpenApiTypes.STR,
            ),
        ],
    )
    def get(request, **_kwargs) -> Response:
        uri = request.query_params.get("uri")
        object_location = request.query_params.get("object_location")
        object_location_schema = request.query_params.get("object_location_schema")
        data = resource_validator(
            uri=uri,
            object_location=object_location,
            object_location_schema=object_location_schema,
        )

        # Check if the request is for XLSX format
        accept_header = request.META.get('HTTP_ACCEPT', '')
        format_is_xlsx = 'application/xlsx' in accept_header or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in accept_header

        return Response(get_return_list(data, format_is_xlsx=format_is_xlsx))
