# Create your views here.
from django.utils.decorators import method_decorator
from drf_renderer_xlsx.mixins import XLSXFileMixin
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_xml.renderers import XMLRenderer

from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from gaodcore_manager.serializers import ConnectorConfigSerializer, ResourceConfigSerializer
from gaodcore_manager.validators import resource_validator
from utils import get_return_list


@method_decorator(name='create', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='update', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='partial_update', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='destroy', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='list', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='retrieve', decorator=swagger_auto_schema(tags=['manager']))
class ConnectorConfigView(XLSXFileMixin, viewsets.ModelViewSet):
    serializer_class = ConnectorConfigSerializer
    queryset = ConnectorConfig.objects.all()
    permission_classes = (IsAuthenticated,)


@method_decorator(name='create', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='update', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='partial_update', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='destroy', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='list', decorator=swagger_auto_schema(tags=['manager']))
@method_decorator(name='retrieve', decorator=swagger_auto_schema(tags=['manager']))
class ResourceConfigView(XLSXFileMixin, viewsets.ModelViewSet):
    serializer_class = ResourceConfigSerializer
    queryset = ResourceConfig.objects.all()
    permission_classes = (IsAuthenticated,)


class ValidatorView(XLSXFileMixin, APIView):
    renderer_classes = [JSONRenderer, XMLRenderer, BrowsableAPIRenderer]
    permission_classes = (IsAuthenticated,)

    @swagger_auto_schema(
        tags=['manager'],
        manual_parameters=[
            openapi.Parameter('uri',
                              openapi.IN_QUERY,
                              required=True,
                              description="URI of resource. Not allowed driver in schema.",
                              type=openapi.TYPE_STRING),
            openapi.Parameter(
                'object_location',
                openapi.IN_QUERY,
                required=False,
                description=
                "This field in databases origins can be a table, view or function. This field in APIs origins "
                "is not required.",
                type=openapi.TYPE_STRING),
            openapi.Parameter('object_location_schema',
                              openapi.IN_QUERY,
                              required=False,
                              description="Schema of object_location. Normally used in databases",
                              type=openapi.TYPE_STRING)
        ])
    def get(self, request, **_kwargs) -> Response:
        uri = request.query_params.get('uri')
        object_location = request.query_params.get('object_location')
        object_location_schema = request.query_params.get('object_location_schema')
        data = resource_validator(uri=uri,
                                  object_location=object_location,
                                  object_location_schema=object_location_schema)
        return Response(get_return_list(data))
