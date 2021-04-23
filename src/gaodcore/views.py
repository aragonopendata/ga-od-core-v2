import json
from json.decoder import JSONDecodeError
from typing import Iterable, Optional, Dict, Any, List

from django.core.serializers.json import DjangoJSONEncoder
from drf_renderer_xlsx.mixins import XLSXFileMixin
from drf_renderer_xlsx.renderers import XLSXRenderer
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import viewsets
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import IsAuthenticated
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.utils.serializer_helpers import ReturnList
from rest_framework.views import APIView
from rest_framework.renderers import JSONRenderer
from rest_framework_csv.renderers import CSVRenderer
from rest_framework_xml.renderers import XMLRenderer
from rest_framework_yaml.renderers import YAMLRenderer

from gaodcore.connectors import get_resource_data, get_resource_columns, NoObjectError, DriverConnectionError, \
    NotImplementedSchemaError, OrderBy, FieldNoExistsError, SortFieldNoExistsError
from gaodcore.models import ConnectorConfig, ResourceConfig
from gaodcore.negotations import LegacyContentNegotiation
from gaodcore.serializers import ConnectorConfigSerializer, ResourceConfigSerializer, DictSerializer
from gaodcore.validators import resource_validator


def get_return_list(data: Iterable[dict]) -> ReturnList:
    return_list = ReturnList(serializer=DictSerializer)

    # FIXME: this convert dates to string, in some renders like xlsx produce a bad representation.
    #  This is required to fixit and find a better solution. :(
    parsed_data = json.loads(json.dumps(list(data), cls=DjangoJSONEncoder))
    for item in parsed_data:
        return_list.append(item)

    return return_list


class ValidatorView(XLSXFileMixin, APIView):
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)
    permission_classes = [IsAuthenticated]

    @swagger_auto_schema(manual_parameters=[
        openapi.Parameter('uri',
                          openapi.IN_QUERY,
                          required=True,
                          description="URI of resource. Not allowed driver in schema.",
                          type=openapi.TYPE_STRING),
        openapi.Parameter(
            'object_location',
            openapi.IN_QUERY,
            required=False,
            description="Can be a table, view or function. Add database schema or anything necessary to reach this"
            "object",
            type=openapi.TYPE_NUMBER)
    ])
    def get(self, request, format=None):
        uri = request.query_params.get('uri')
        object_location = request.query_params.get('object_location')
        data = resource_validator(uri=uri, object_location=object_location)
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
    content_negotiation_class = LegacyContentNegotiation

    @swagger_auto_schema(manual_parameters=[
        openapi.Parameter('resource_id',
                          openapi.IN_QUERY,
                          description="Id of resource to be searched against.",
                          type=openapi.TYPE_NUMBER),
        openapi.Parameter('view_id', openapi.IN_QUERY, description="Alias of resource_id.", type=openapi.TYPE_NUMBER),
        openapi.Parameter('filters',
                          openapi.IN_QUERY,
                          description='matching conditions to select, e.g '
                          '{“key1”: “a”, “key2”: “b”}.',
                          type=openapi.TYPE_OBJECT),
        openapi.Parameter('offset',
                          openapi.IN_QUERY,
                          description="Offset this number of rows.",
                          type=openapi.TYPE_INTEGER),
        openapi.Parameter('fields',
                          openapi.IN_QUERY,
                          description="Fields to return. Default: all fields in original order.",
                          type=openapi.TYPE_ARRAY,
                          items=openapi.Items(type=openapi.TYPE_STRING)),
        openapi.Parameter('columns',
                          openapi.IN_QUERY,
                          description="Alias of fields",
                          type=openapi.TYPE_ARRAY,
                          items=openapi.Items(type=openapi.TYPE_STRING)),
        openapi.Parameter('sort',
                          openapi.IN_QUERY,
                          description="comma separated field names with ordering e.g.: "
                          "“fieldname1, fieldname2 desc”",
                          type=openapi.TYPE_ARRAY,
                          items=openapi.Items(type=openapi.TYPE_STRING)),
        openapi.Parameter('formato',
                          openapi.IN_QUERY,
                          description='Backward compatibility of "Accept" header or extension.',
                          type=openapi.TYPE_STRING),
        openapi.Parameter('nameRes',
                          openapi.IN_QUERY,
                          description='Force name of file to download.',
                          type=openapi.TYPE_STRING),
    ])
    def get(self, request: Request, format=None):
        resource_id = self._get_resource_id(request)
        offset = self._get_offset(request)
        limit = self._get_limit(request)
        fields = request.query_params.getlist('fields') or request.query_params.getlist('columns', [])
        filters = self._get_filters(request)
        sort = self._get_sort(request)

        try:
            resource_config = ResourceConfig.objects.select_related().get(id=resource_id,
                                                                          enabled=True,
                                                                          connector_config__enabled=True)
        except ResourceConfig.DoesNotExist:
            raise ValidationError("Resource not exists or is not available", 400)

        try:
            data = get_resource_data(uri=resource_config.connector_config.uri,
                                     object_location=resource_config.object_location,
                                     filters=filters,
                                     limit=limit,
                                     offset=offset,
                                     fields=fields,
                                     sort=sort)
        except (FieldNoExistsError, SortFieldNoExistsError) as err:
            raise ValidationError(err, 400)
        except NoObjectError:
            raise ValidationError(f'Object "{resource_config.object_location}" is not available.', 500)
        except DriverConnectionError:
            raise ValidationError('Connection is not available.', 500)
        except NotImplementedSchemaError as err:
            raise ValidationError(str(err), 500)

        response = Response(get_return_list(data))

        if request.get_full_path().startswith('/gaodcore/download'):
            filename = request.query_params.get('nameRes') or resource_config.name
            disposition = f'attachment; filename="{filename}.{request.accepted_renderer.format}"'
            response['Content-Disposition'] = disposition

        return response

    @staticmethod
    def _get_resource_id(request: Request) -> int:
        try:
            resource_id = int(request.query_params.get('resource_id') or request.query_params.get('view_id'))
        except ValueError:
            raise ValidationError("Resource_id is not a number.")

        if not resource_id:
            raise ValidationError("Is required specify resource_id in query string.")
        return resource_id

    @staticmethod
    def _get_offset(request: Request) -> int:
        try:
            offset = int(request.query_params.get('offset') or 0)
        except ValueError:
            raise ValidationError("Value of offset is not a number.", 400)
        return offset

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

    @staticmethod
    def _get_limit(request: Request) -> Optional[int]:
        limit = request.query_params.get('limit') or None
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                raise ValidationError("Value of limit is not a number.", 400)
        return limit

    @staticmethod
    def _get_filters(request: Request) -> Dict[str, Any]:
        try:
            filters = json.loads(request.query_params.get('filters', '{}'))
        except JSONDecodeError:
            raise ValidationError('Invalid JSON.', 400)

        if type(filters) != dict:
            raise ValidationError("Invalid format: eg. {“key1”: “a”, “key2”: “b”}.", 400)

        for key, value in filters.items():
            if type(value) not in (str, int, float, bool, None) and value is not None:
                raise ValidationError(f'Value {value} is not a String, Integer, Float, Bool, Null or None', 400)
        return filters

    @staticmethod
    def _get_sort(request: Request) -> List[OrderBy]:
        sort = []
        for item in request.query_params.get('sort', '').strip().split(','):
            item: str
            if not item:
                continue
            clausule = item.strip().split(' ')
            if len(clausule) == 1:
                sort.append(OrderBy(field=clausule[0], ascending=True))
            elif len(clausule) == 2:
                if clausule[1].lower() == 'asc':
                    sort.append(OrderBy(field=clausule[0], ascending=True))
                elif clausule[1].lower() == 'desc':
                    sort.append(OrderBy(field=clausule[0], ascending=False))
                else:
                    raise ValidationError(f'Sort value "{item}" is not allowed. Ej: “fieldname1 asc, fieldname2 desc”.')
            else:
                raise ValidationError(f'Sort value "{item}" is not allowed. Too many arguments.')

        return sort


class ShowColumnsView(XLSXFileMixin, APIView):
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)

    @swagger_auto_schema(manual_parameters=[
        openapi.Parameter('resource_id', openapi.IN_QUERY, description="", type=openapi.TYPE_NUMBER),
        openapi.Parameter('view_id', openapi.IN_QUERY, description="Alias of resource_id", type=openapi.TYPE_NUMBER),
    ])
    def get(self, request: Request, format=None):
        resource_id = request.query_params.get('resource_id') or request.query_params.get('view_id')
        resource_config = ResourceConfig.objects.select_related().get(id=resource_id,
                                                                      enabled=True,
                                                                      connector_config__enabled=True)
        try:
            data = get_resource_columns(uri=resource_config.connector_config.uri,
                                        object_location=resource_config.object_location)
        except NoObjectError:
            raise ValidationError(f'Object "{resource_config.object_location}" is not available.', 500)
        except DriverConnectionError:
            raise ValidationError('Connection is not available.', 500)
        except NotImplementedSchemaError as err:
            raise ValidationError(str(err), 500)

        return Response(get_return_list(data))


class ResourcesView(XLSXFileMixin, APIView):
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)

    def get(self, request: Request, format=None):
        resources = ({
            'resource_id': resource.id,
            'resource_name': resource.name
        } for resource in ResourceConfig.objects.all())
        return Response(get_return_list(resources))
