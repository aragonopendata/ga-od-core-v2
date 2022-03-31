import json
import io
import csv
from json.decoder import JSONDecodeError
from typing import Optional, Dict, Any, List, Callable

from drf_renderer_xlsx.mixins import XLSXFileMixin
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.exceptions import ValidationError
from rest_framework.request import Request
from rest_framework.response import Response
from connectors import get_resource_data, get_resource_columns, NoObjectError, DriverConnectionError, \
    NotImplementedSchemaError, OrderBy, FieldNoExistsError, SortFieldNoExistsError, MimeTypeError, \
    validator_max_excel_allowed, TooManyRowsErrorExcel, get_resource_data_feature
from gaodcore.negotations import LegacyContentNegotiation
from gaodcore_manager.models import ResourceConfig
from utils import get_return_list
from views import APIViewMixin
import xlsxwriter
import io
from django.http import HttpResponse 
from rest_framework.utils.serializer_helpers import ReturnList


_RESOURCE_MAX_ROWS_EXCEL = 1048576

def _get_data_public_error(func: Callable, *args, **kwargs) -> List[Dict[str, Any]]:
    try:
        return func(*args, **kwargs)
    except (FieldNoExistsError, SortFieldNoExistsError) as err:
        raise ValidationError(err, 400) from err
    except NoObjectError as err:
        raise ValidationError('Object is not available.', 500) from err
    except DriverConnectionError as err:
        raise ValidationError('Connection is not available.', 500) from err
    except NotImplementedSchemaError as err:
        raise ValidationError("Unexpected error: schema is not implemented.", 500) from err
    except MimeTypeError as err:
        raise ValidationError("Unexpected error: mimetype of input file is not implemented.", 500) from err


def _get_resource(resource_id: int):
    try:
        return ResourceConfig.objects.select_related().get(id=resource_id, enabled=True, connector_config__enabled=True)
    except ResourceConfig.DoesNotExist as err:
        raise ValidationError("Resource not exists or is not available", 400) from err

def get_response_xlsx(data: ReturnList)-> HttpResponse:
    """Get resource XLSX with order column names."""
    """output XLSX (Comma Separated Values) dynamically using Django views"""
    """columns_order XlsxWriter can be used to write text, numbers, formulas and hyperlinks to multiple"""
    """worksheets and it supports features such as formatting and many more, includin """
      
        
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output)
    worksheet = workbook.add_worksheet()
        
    #column header names, you can use your own headers here
        
    for row, item in enumerate(data):
        for col, (key, value) in enumerate(item.items()):
            worksheet.write(row+1,col,value)
            if row==0:
                worksheet.write(row,col,key)
                     
    # Close the workbook before sending the data.
    workbook.close()    
    output.seek(0)
           
    return(HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
    

def get_response_csv(data:ReturnList)-> HttpResponse:
        
    """Get resource csv with order column names."""
    """output CSV (Comma Separated Values) dynamically using Django views"""
       
    response = HttpResponse(content_type='text/csv')
    writer = csv.writer(response)
         
    for row, item in enumerate(data):
        csv_header = []
        csv_data = []
        for col, (key, value) in enumerate(item.items()):
            if row==0:
                csv_header.append(key)
            csv_data.append(value)
        if row==0:
            writer.writerow(csv_header)  
        writer.writerow(csv_data)  
    return response

class DownloadView(APIViewMixin):
    """This view allow get public serialized data from internal databases or APIs of Gobierno de Aragón."""
    _PREVIEW_LIMIT = 1000
    _DOWNLOAD_ENDPOINT = ('/GA_OD_Core/download', '/GA_OD_Core/download')

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
                             openapi.Parameter('like',
                                               openapi.IN_QUERY,
                                               description='Matching conditions to select, e.g '
                                               '{“key1”: “a”, “key2”: “b”}.',
                                               type=openapi.TYPE_OBJECT),
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
                             openapi.Parameter('name',
                                               openapi.IN_QUERY,
                                               description='Force name of file to download.',
                                               type=openapi.TYPE_STRING),
                             openapi.Parameter(
                                 '_page',
                                 openapi.IN_QUERY,
                                 description='Deprecated. Number of the page.',
                                 type=openapi.TYPE_INTEGER,
                             ),
                             openapi.Parameter('_pageSize',
                                               openapi.IN_QUERY,
                                               description='Deprecated. Number of results in each page.',
                                               type=openapi.TYPE_INTEGER),
                         ])
    def get(self, request: Request, **_kwargs) -> Response:
        """This method allows get serialized public data from databases or APIs of Gobierno de Aragón."""
        resource_id = self._get_resource_id(request)
        offset = self._get_offset(request)
        limit = self._get_limit(request)
        fields = request.query_params.getlist('fields') or request.query_params.getlist('columns', [])
        filters = self._get_filters(request)
        like = self._get_like(request)
        sort = self._get_sort(request)
        format= self._get_format(request)
        
        resource_config = _get_resource(resource_id=resource_id)

        if format == "xlsx":
            if not validator_max_excel_allowed(uri=resource_config.connector_config.uri,
                                       object_location=resource_config.object_location,
                                       object_location_schema=resource_config.object_location_schema,
                                       filters=filters,
                                       like=like,
                                       limit=limit,
                                       offset=offset,
                                       fields=fields,
                                       sort=sort) :
                raise ValidationError("An xlsx cannot be generated with so many lines, please request it in another format", 407) from TooManyRowsErrorExcel
           #    raise ValidationError({'error' : 'An xlsx cannot be generated with so many lines, please request it in another format'})    

   
        
        
        reourceGeojon = get_GeoJson_resource(uri=resource_config.connector_config.uri,object_location=resource_config.object_location,object_location_schema=resource_config.object_location_schema)
        if reourceGeojon and format == 'json':
            featureCollection= True
        else:
            featureCollection = False

        if featureCollection:
            data = _get_data_public_error(get_resource_data_featrure,
                                      uri=resource_config.connector_config.uri,
                                      object_location=resource_config.object_location,
                                      object_location_schema=resource_config.object_location_schema,
                                      filters=filters,
                                      like=like,
                                      limit=limit,
                                      offset=offset,
                                      fields=fields,
                                      sort=sort)
            
        
        else:
            data = _get_data_public_error(get_resource_data,
                                      uri=resource_config.connector_config.uri,
                                      object_location=resource_config.object_location,
                                      object_location_schema=resource_config.object_location_schema,
                                      filters=filters,
                                      like=like,
                                      limit=limit,
                                      offset=offset,
                                      fields=fields,
                                      sort=sort)
        
        
        if format == "xlsx":
            response = get_response_xlsx(get_return_list(data))
        elif format == "csv":  
            response = get_response_csv(get_return_list(data))
        elif featureCollection:
             response = Response(data)
        else: 
            response = Response(get_return_list(data))
        
        if self.is_download_endpoint(request) or format == "xlsx":
            filename = request.query_params.get('name') or request.query_params.get('nameRes') or resource_config.name
            disposition = f'attachment; filename="{filename}.{request.accepted_renderer.format}"'
            response["content-disposition"] = disposition
            
                

        return response
    

    def get_filename(self, request: Request, resource_config: ResourceConfig):
        """Note: this is import due that replace XLSX Render method that forcer his own filename"""
        return request.query_params.get('name') or request.query_params.get('nameRes') or resource_config.name

    def is_download_endpoint(self, request: Request):
        return any((request.get_full_path().startswith(endpoint) for endpoint in self._DOWNLOAD_ENDPOINT))

    @staticmethod
    def _get_resource_id(request: Request) -> int:
        """Get resource_id from query string.

        @param request: Django response instance.
        @return: resource_id.
        """
        try:
            resource_id = int(request.query_params.get('resource_id') or request.query_params.get('view_id'))
        except ValueError as err:
            raise ValidationError("Resource_id is not a number.") from err

        if not resource_id:
            raise ValidationError("Is required specify resource_id in query string.")
        return resource_id


    @staticmethod
    def _get_int_field(request: Request, field) -> int:
        """Get interger field from query string.

        @param request: Django response instance.
        @return: SQL offset value.
        """
        value = request.query_params.get(field)
        if value:
            try:
                value = int(request.query_params.get(field))
            except ValueError as err:
                raise ValidationError(f"Value of {field} is not a number.", 400) from err

        return value

    def _get_offset(self, request: Request) -> int:
        """Get offset from query string.

        @param request: Django response instance.
        @return: SQL offset value.
        """

        offset = self._get_int_field(request, 'offset')
        page = self._get_int_field(request, '_page')
        page_size = self._get_int_field(request, '_pageSize')
        
        try:
            if page > 0 and page_size:
                offset = (page -1) * page_size
        except TypeError:
            offset = 0
        
        return offset

    def _get_limit(self, request: Request) -> Optional[int]:
        """Get limit from query string. If preview, limit number of rows.

        @param request: Django response instance.
        @return: SQL limit value.
        """
        limit = request.query_params.get('limit') or None
        page_size = self._get_int_field(request, '_pageSize')
      
        if limit:
            try:
                limit = int(limit)
            except ValueError as err:
                raise ValidationError("Value of limit is not a number.", 400) from err

            if request.get_full_path().startswith('/preview'):
                if limit > self._PREVIEW_LIMIT:
                    limit = self._PREVIEW_LIMIT
        elif page_size:
            try:
                limit = int(page_size) 
            except ValueError as err:
                raise ValidationError("Value of _pageSize is not a number.", 400) from err
            
            if request.get_full_path().startswith('/preview'):
                 if page_size > self._PREVIEW_LIMIT:
                    limit = self._PREVIEW_LIMIT
        
           
        return limit

    @staticmethod
    def _get_filters(request: Request) -> Dict[str, Any]:
        """Get filters from query string.

        @param request: Django response instance.
        @return: filters. SQL where parameters. Format {"column": value, ...}.
        """
        try:
            filters = json.loads(request.query_params.get('filters', '{}'))
        except JSONDecodeError as err:
            raise ValidationError('Invalid JSON.', 400) from err

        if not isinstance(filters, dict):
            raise ValidationError('Invalid format: eg. {“key1”: “a”, “key2”: “b”}', 400)

        for _, value in filters.items():
            if type(value) not in (str, int, float, bool, None) and value is not None:
                raise ValidationError(f'Value {value} is not a String, Integer, Float, Bool, Null or None', 400)
        return filters
    
    @staticmethod
    def _get_like(request: Request) -> Dict[str, Any]:
        """Get filters_like from query string.

        @param request: Django response instance.
        @return: filters_like. SQL where parameters. Format {"column": value, ...}.
        """
        try:
            like = json.loads(request.query_params.get('like', '{}'))
           
        except JSONDecodeError as err:
            raise ValidationError('Invalid JSON.', 400) from err

        if not isinstance(like, dict):
            raise ValidationError('Invalid format: eg. {“key1”: “a”, “key2”: “b”}', 400)
        for _, value in like.items():
            if type(value) not in (str, int, float, bool, None) and value is not None:
                raise ValidationError(f'Value {value} is not a String, Integer, Float, Bool, Null or None', 400)
        return request.query_params.get('like')
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
                    raise ValidationError(f'Sort value {item} is not allowed. Ej: fieldname1 asc, fieldname2 desc.')
            else:
                raise ValidationError(f'Sort value {item} is not allowed. Too many arguments.')

        return sort
    
    @staticmethod
    def _get_format(request: Request) -> str:
        """Get Response_type from query string.

        @param request: Django response instance.
        @return:Response_content_type.
        """
        try:
            format = request.accepted_renderer.format
        except ValueError as err:
            raise ValidationError('Invalid Response_contnt_type.', 400) from err

        return format

class ShowColumnsView(XLSXFileMixin, APIViewMixin):
    """This view allows to get datatype of each column from a resource."""
    @staticmethod
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
    def get(request: Request, **_kwargs) -> Response:
        """This method allows to get datatype of each column from a resource."""
        resource_id = request.query_params.get('resource_id') or request.query_params.get('view_id')
        resource_config = _get_resource(resource_id=resource_id)
        data = _get_data_public_error(get_resource_columns,
                                      uri=resource_config.connector_config.uri,
                                      object_location=resource_config.object_location,
                                      object_location_schema=resource_config.object_location_schema)

        return Response(get_return_list(data))


class ResourcesView(XLSXFileMixin, APIViewMixin):  # pylint: disable=too-few-public-methods
    """This view allow to get a list of public resources."""
    @staticmethod
    @swagger_auto_schema(tags=['default'])
    def get(_: Request, **_kwargs) -> Response:
        """This view allow to get a list of public resources."""
        resources = ({
            'id': resource.id,
            'name': resource.name,
            'available': resource.enabled and resource.connector_config.enabled,
        } for resource in ResourceConfig.objects.order_by("id").prefetch_related('connector_config').all())
        return Response(get_return_list(resources))
