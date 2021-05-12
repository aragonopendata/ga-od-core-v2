import json
from json.decoder import JSONDecodeError
from typing import Optional, Dict, Any, List

from drf_renderer_xlsx.mixins import XLSXFileMixin
from drf_renderer_xlsx.renderers import XLSXRenderer
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.exceptions import ValidationError
from rest_framework.renderers import JSONRenderer
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_csv.renderers import CSVRenderer
from rest_framework_xml.renderers import XMLRenderer
from rest_framework_yaml.renderers import YAMLRenderer

from connectors import get_resource_data, get_resource_columns, NoObjectError, DriverConnectionError, \
    NotImplementedSchemaError, OrderBy, FieldNoExistsError, SortFieldNoExistsError
from gaodcore.negotations import LegacyContentNegotiation
from gaodcore_manager.models import ResourceConfig
from utils import get_return_list
from views import APIViewMixin


class DownloadView(XLSXFileMixin, APIViewMixin):
    """This view allow get public serialized data from internal databases or APIs of Gobierno de Aragón."""

    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)
    content_negotiation_class = LegacyContentNegotiation

    @swagger_auto_schema(tags=['default'],
                         manual_parameters=[
                             openapi.Parameter('resource_id',
                                               openapi.IN_QUERY,
                                               description="Id of resource to be searched against.",
                                               type=openapi.TYPE_NUMBER),
                             openapi.Parameter('view_id',
                                               openapi.IN_QUERY,
                                               description="Alias of resource_id. Backward compatibility.",
                                               type=openapi.TYPE_NUMBER),
                             openapi.Parameter('filters',
                                               openapi.IN_QUERY,
                                               description='Matching conditions to select, e.g '
                                               '{“key1”: “a”, “key2”: “b”}.',
                                               type=openapi.TYPE_OBJECT),
                             openapi.Parameter('offset',
                                               openapi.IN_QUERY,
                                               description="Offset this number of rows.",
                                               type=openapi.TYPE_INTEGER),
                             openapi.Parameter('limit',
                                               openapi.IN_QUERY,
                                               description="Limit this number of rows.",
                                               type=openapi.TYPE_INTEGER),
                             openapi.Parameter('fields',
                                               openapi.IN_QUERY,
                                               description="Fields to return. Default: all fields in original order.",
                                               type=openapi.TYPE_ARRAY,
                                               items=openapi.Items(type=openapi.TYPE_STRING)),
                             openapi.Parameter('columns',
                                               openapi.IN_QUERY,
                                               description="Alias of fields.",
                                               type=openapi.TYPE_ARRAY,
                                               items=openapi.Items(type=openapi.TYPE_STRING)),
                             openapi.Parameter('sort',
                                               openapi.IN_QUERY,
                                               description="Comma separated field names with ordering e.g: "
                                               "“fieldname1, fieldname2 desc”.",
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
    def get(self, request: Request, **_kwargs) -> Response:
        """This method allows get serialized public data from databases or APIs of Gobierno de Aragón."""
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
                                     object_location_schema=resource_config.object_location_schema,
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

        if request.get_full_path().startswith('/download'):
            filename = request.query_params.get('nameRes') or resource_config.name
            disposition = f'attachment; filename="{filename}.{request.accepted_renderer.format}"'
            response['Content-Disposition'] = disposition

        return response

    @staticmethod
    def _get_resource_id(request: Request) -> int:
        """Get resource_id from query string.

        @param request: Django response instance.
        @return: resource_id.
        """
        try:
            resource_id = int(request.query_params.get('resource_id') or request.query_params.get('view_id'))
        except ValueError:
            raise ValidationError("Resource_id is not a number.")

        if not resource_id:
            raise ValidationError("Is required specify resource_id in query string.")
        return resource_id

    @staticmethod
    def _get_offset(request: Request) -> int:
        """Get offset from query string.

        @param request: Django response instance.
        @return: SQL offset value.
        """
        try:
            offset = int(request.query_params.get('offset') or 0)
        except ValueError:
            raise ValidationError("Value of offset is not a number.", 400)
        return offset

    @staticmethod
    def _get_limit(request: Request) -> Optional[int]:
        """Get limit from query string.

        @param request: Django response instance.
        @return: SQL limit value.
        """
        limit = request.query_params.get('limit') or None
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                raise ValidationError("Value of limit is not a number.", 400)
        return limit

    @staticmethod
    def _get_filters(request: Request) -> Dict[str, Any]:
        """Get filters from query string.

        @param request: Django response instance.
        @return: filters. SQL where parameters. Format {"column": value, ...}.
        """
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
        """Get sort options from query string.

        @param request: Django response instance.
        @return: List of OrderBy. This are used in SQL sentences.
        """
        sort = []
        for item in request.query_params.get('sort', '').strip().split(','):
            item: str
            if not item:
                continue
            clause = item.strip().split(' ')
            if len(clause) == 1:
                sort.append(OrderBy(field=clause[0], ascending=True))
            elif len(clause) == 2:
                if clause[1].lower() == 'asc':
                    sort.append(OrderBy(field=clause[0], ascending=True))
                elif clause[1].lower() == 'desc':
                    sort.append(OrderBy(field=clause[0], ascending=False))
                else:
                    raise ValidationError(f'Sort value "{item}" is not allowed. Ej: “fieldname1 asc, fieldname2 desc”.')
            else:
                raise ValidationError(f'Sort value "{item}" is not allowed. Too many arguments.')

        return sort


class ShowColumnsView(XLSXFileMixin, APIView):
    """This view allows to get datatype of each column from a resource."""
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)

    @swagger_auto_schema(tags=['default'],
                         manual_parameters=[
                             openapi.Parameter('resource_id',
                                               openapi.IN_QUERY,
                                               description="",
                                               type=openapi.TYPE_NUMBER),
                             openapi.Parameter('view_id',
                                               openapi.IN_QUERY,
                                               description="Alias of resource_id. Backward compatibility.",
                                               type=openapi.TYPE_NUMBER),
                         ])
    def get(self, request: Request, **_kwargs) -> Response:
        """This method allows to get datatype of each column from a resource."""
        resource_id = request.query_params.get('resource_id') or request.query_params.get('view_id')
        resource_config = ResourceConfig.objects.select_related().get(id=resource_id,
                                                                      enabled=True,
                                                                      connector_config__enabled=True)
        try:
            data = get_resource_columns(uri=resource_config.connector_config.uri,
                                        object_location=resource_config.object_location,
                                        object_location_schema=resource_config.object_location_schema)
        except NoObjectError:
            raise ValidationError(f'Object "{resource_config.object_location}" is not available.', 500)
        except DriverConnectionError:
            raise ValidationError('Connection is not available.', 500)
        except NotImplementedSchemaError as err:
            raise ValidationError(str(err), 500)

        return Response(get_return_list(data))


class ResourcesView(XLSXFileMixin, APIView):
    """This view allow to get a list of public resources."""
    renderer_classes = (JSONRenderer, XLSXRenderer, YAMLRenderer, XMLRenderer, CSVRenderer)

    @swagger_auto_schema(
        tags=['default'], )
    def get(self, _: Request, **_kwargs) -> Response:
        """This view allow to get a list of public resources."""
        resources = ({
            'resource_id': resource.id,
            'resource_name': resource.name,
            'available': resource.enabled,
        } for resource in ResourceConfig.objects.all())
        return Response(get_return_list(resources))
