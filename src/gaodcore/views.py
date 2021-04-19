import json
from typing import List

from django.core.serializers.json import DjangoJSONEncoder
from drf_renderer_xlsx.mixins import XLSXFileMixin
from drf_renderer_xlsx.renderers import XLSXRenderer
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.utils.serializer_helpers import ReturnList, ReturnDict
from rest_framework.views import APIView
from rest_framework.renderers import JSONRenderer
from rest_framework_csv.renderers import CSVRenderer
from rest_framework_xml.renderers import XMLRenderer
from rest_framework_yaml.renderers import YAMLRenderer

from gaodcore.connectors import get_resource_data, get_resource_columns
from gaodcore.models import ConnectorConfig, ResourceConfig
from gaodcore.serializers import ConnectorConfigSerializer, ResourceConfigSerializer, DictSerializer


def get_return_list(data: List[dict]) -> ReturnList:
    return_list = ReturnList(serializer=DictSerializer)

    # FIXME: this convert dates to string, in some renders like xlsx produce a bad representation.
    #  This is required to fixit and find a better solution. :(
    parsed_data = json.loads(json.dumps(data, cls=DjangoJSONEncoder))
    for item in parsed_data:
        return_list.append(item)

    return return_list


class ValidatorView(XLSXFileMixin, APIView):
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)
    permission_classes = [IsAuthenticated]

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                'uri',
                openapi.IN_QUERY,
                required=True,
                description="URI of resource. Not allowed driver in schema.", type=openapi.TYPE_STRING),
            openapi.Parameter(
                'object_location',
                openapi.IN_QUERY,
                required=False,
                description="Can be a table, view or function. Add database schema or anything necessary to reach this"
                            "object", type=openapi.TYPE_NUMBER)])
    def get(self, request, format=None):
        uri = request.query_params.get('uri')
        object_location = request.query_params.get('object_location')
        data = get_resource_data(uri=uri, object_location=object_location, filter_by={}, cache=False, fields=[])
        return Response(get_return_list(data))


class ConnectorConfigView(XLSXFileMixin, viewsets.ModelViewSet):
    serializer_class = ConnectorConfigSerializer
    queryset = ConnectorConfig.objects.all()
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)
    permission_classes = [IsAuthenticated]


class ResourceConfigView(XLSXFileMixin, viewsets.ModelViewSet):
    serializer_class = ResourceConfigSerializer
    queryset = ResourceConfig.objects.all()
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)
    permission_classes = [IsAuthenticated]


class DownloadView(XLSXFileMixin, APIView):
    """
    List all snippets, or create a new snippet.
    """
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter('resource_id', openapi.IN_QUERY, description="", type=openapi.TYPE_NUMBER),
            openapi.Parameter('view_id', openapi.IN_QUERY, description="Alias of resource_id",
                              type=openapi.TYPE_NUMBER),
            openapi.Parameter('fields', openapi.IN_QUERY, description="", type=openapi.TYPE_ARRAY, items=openapi.Items(type=openapi.TYPE_STRING)),
            openapi.Parameter('columns', openapi.IN_QUERY, description="Alias of fields",
                              type=openapi.TYPE_ARRAY, items=openapi.Items(type=openapi.TYPE_STRING)),
        ]
    )
    def get(self, request: Request, format=None):
        resource_id = request.query_params.get('resource_id') or request.query_params.get('view_id')
        if not resource_id:
            raise  # TODO: add a error and return a description to the users

        fields = request.query_params.getlist('fields') or request.query_params.getlist('columns', [])

        resource_config = ResourceConfig.objects.select_related().get(
            id=resource_id, enabled=True, connector_config__enabled=True)
        data = get_resource_data(uri=resource_config.connector_config.uri,
                                 object_location=resource_config.object_location,
                                 filter_by={},
                                 fields=fields)  # TODO: notify if failed

        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser


class ShowColumnsView(XLSXFileMixin, APIView):
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter('resource_id', openapi.IN_QUERY, description="", type=openapi.TYPE_NUMBER),
            openapi.Parameter('view_id', openapi.IN_QUERY, description="Alias of resource_id",
                              type=openapi.TYPE_NUMBER),
        ]
    )
    def get(self, request: Request, format=None):
        resource_id = request.query_params.get('resource_id') or request.query_params.get('view_id')
        resource_config = ResourceConfig.objects.select_related().get(
            id=resource_id, enabled=True, connector_config__enabled=True)
        data = get_resource_columns(uri=resource_config.connector_config.uri,
                                    object_location=resource_config.object_location)  # TODO: notify if failed

        return Response(get_return_list(data))


class ResourcesView(XLSXFileMixin, APIView):
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)

    def get(self, request: Request, format=None):
        return_dict = ReturnDict(serializer=DictSerializer)
        for resource in ResourceConfig.objects.all():
            return_dict[resource.id] = resource.name

        return Response(return_dict)
