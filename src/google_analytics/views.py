import httplib2
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Iterable, List

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

from rest_framework.request import Request
from rest_framework.response import Response

from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page

from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema

from django.shortcuts import render
from gaodcore_project.settings import CONFIG


from views import APIViewMixin


class APIViewGetDataMixin(APIViewMixin, metaclass=ABCMeta):  # pylint: disable=too-few-public-methods
    """Mixin of helpers that helps the generations Aragon transports views."""
    _DATA_FIELD = 'items'
    _OPTIONS = {}

    def _get_options(self):
        return self._OPTIONS

    @property
    @abstractmethod
    def _ENDPOINT(self) -> str:
        pass

    @property
    @abstractmethod
    def _FLATTEN(self) -> str:
        pass

    def _get_default_endpoint_data(self) -> List[Dict[str, Any]]:
        url = CONFIG.projects.transport.aragon.get_url(self._ENDPOINT,
                                                       customer_id=CONFIG.projects.transport.aragon.customer_id)
        return self._get_endpoint_data(url)

    def _get_endpoint_data(self, url: str) -> List[Dict[str, Any]]:
        data = download(url,
                        auth=requests.auth.HTTPBasicAuth(CONFIG.projects.transport.aragon.user,
                                                         CONFIG.projects.transport.aragon.password))
        return data[self._DATA_FIELD]

    def _get_data(self) -> Iterable[Dict[str, Any]]:
        data = self._get_default_endpoint_data()
        if self._FLATTEN:
            data = (flatten_dict(row) for row in data)
        return data


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['google_analytics']))
class GoogleAnalyticsView(APIViewMixin):
    """Returns a query to google analytics API.
    https://developers.google.com/apis-explorer/#p/analytics/v3/
    Using:
    https://developers.google.com/apis-explorer/#p/analytics/v3/analytics.data.ga.get
     Args:
        start_date(String): Requests can specify a start date formatted as YYYY-MM-DD, or as a relative date (e.g., today, yesterday, or 7daysAgo). The default value is 7daysAgo. (string)
        end_date(String): Requests can specify a start date formatted as YYYY-MM-DD, or as a relative date (e.g., today, yesterday, or 7daysAgo). The default value is 7daysAgo. (string)
        metrics(String): A comma-separated list of Analytics metrics. E.g., 'ga:sessions,ga:pageviews'. At least one metric must be specified. (string)
        dimensions(String):A comma-separated list of Analytics dimensions. E.g., 'ga:browser,ga:city'.
        filters(String):A comma-separated list of dimension or metric filters to be applied to Analytics data.
        include-empty-rows(Boolean):The response will include empty rows if this parameter is set to true, the default is true
        max-results(Integer):The maximum number of entries to include in this feed.
        output(String):The selected format for the response. Default format is JSON.
        samplingLevel(String):The desired sampling level.
        segment(String):An Analytics segment to be applied to data.
        sort(String):A comma-separated list of dimensions or metrics that determine the sort order for Analytics data.
        start-index(integer, 1+):An index of the first entity to retrieve. Use this parameter as a pagination mechanism along with the max-results parameter.
        fields:Selector specifying which fields to include in a partial response.
    Returns:
        reultados(Dictionary): Array in dictionary format.
    """
    _ENDPOINT = 'analytics'
    _FIELD_TAGS = '1'
    _FIELD_STATUS = 'status'
    _FIELD_STATUS_SOLD = 'SOLD'
    _FLATTEN = False

    _START_DATE = 'start_date'
    _END_DATE = 'end_date'
    _METRICS = 'metrics'
    _DIMENSIONS = 'dimensions'
    _FILTERS = 'filters'
    _INCLUDE_EMPTY_ROWS = 'include_empty_rows'
    _MAX_RESULTS = 'max_results'
    _OUTPUT =  'output'
    _SAMPLINGLEVEL = 'samplingLevel'
    _SEGMENT = 'segment'
    _SORT =  'sort'
    _START_INDEX = 'start_index'
    # _FIELDS = 'fields'

    _DEFAULT_START_DATE =  '7daysAgo'
    _DEFAULT_END_DATE =  '7daysAgo'
    _DEFAULT_METRICS =  'ga:pageviews'
    _DEFAULT_DIMENSIONS =  'ga:city'
    _DEFAULT_FILTERS =  ''  # filters(String):A comma-separated list of dimension or metric filters to be applied to Analytics data.
    _DEFAULT_INCLUDE_EMPTY_ROWS =  True
    _DEFAULT_MAX_RESULTS =  100
    _DEFAULT_OUTPUT =  'json'
    _DEFAULT_SAMPLINGLEVEL =  'samplingLevel'  # samplingLevel(String):The desired sampling level.
    _DEFAULT_SEGMENT =  'segment'  # segment(String):An Analytics segment to be applied to data.
    _DEFAULT_SORT =  'sort'  # sort(String):A comma-separated list of dimensions or metrics that determine the sort order for Analytics data.
    _DEFAULT_START_INDEX =  0
    # _DEFAULT_FIELDS =  'fields'  # fields:Selector specifying which fields to include in a partial response.

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @swagger_auto_schema(manual_parameters=[
        openapi.Parameter('start_date', openapi.IN_QUERY,
                          description="start date formatted as YYYY-MM-DD, or as a relative date (e.g., today, yesterday, or 7daysAgo). The default value is 7daysAgo.",
                          type=openapi.TYPE_STRING),
        openapi.Parameter('end_date', openapi.IN_QUERY,
                          description="end date formatted as YYYY-MM-DD, or as a relative date (e.g., today, yesterday, or 7daysAgo). The default value is 7daysAgo.",
                          type=openapi.TYPE_STRING),
        openapi.Parameter('metrics', openapi.IN_QUERY,
                          description="A comma-separated list of Analytics metrics. E.g., 'ga:sessions,ga:pageviews'. At least one metric must be specified.",
                          type=openapi.TYPE_STRING),
        openapi.Parameter('dimensions', openapi.IN_QUERY,
                          description="A comma-separated list of Analytics dimensions. E.g., 'ga:browser,ga:city'.",
                          type=openapi.TYPE_STRING),
        openapi.Parameter('filters', openapi.IN_QUERY,
                          description="A comma-separated list of dimension or metric filters to be applied to Analytics data. ",
                          type=openapi.TYPE_STRING),
        openapi.Parameter('include_empty_rows', openapi.IN_QUERY,
                          description="The response will include empty rows if this parameter is set to true, the default is true ",
                          type=openapi.TYPE_BOOLEAN),
        openapi.Parameter('max_results', openapi.IN_QUERY,
                          description="The maximum number of entries to include in this feed.",
                          type=openapi.TYPE_NUMBER),
        openapi.Parameter('output', openapi.IN_QUERY,
                          description="The selected format for the response. Default format is JSON. ",
                          type=openapi.TYPE_STRING),
        openapi.Parameter('samplingLevel', openapi.IN_QUERY, description="The desired sampling level.",
                          type=openapi.TYPE_STRING),
        openapi.Parameter('segment', openapi.IN_QUERY, description="An Analytics segment to be applied to data.",
                          type=openapi.TYPE_STRING),
        openapi.Parameter('sort', openapi.IN_QUERY,
                          description="A comma-separated list of dimensions or metrics that determine the sort order for Analytics data. ",
                          type=openapi.TYPE_STRING),
        openapi.Parameter('start_index', openapi.IN_QUERY,
                          description="An index of the first entity to retrieve. Use this parameter as a pagination mechanism along with the max-results parameter.",
                          type=openapi.TYPE_NUMBER),
        # openapi.Parameter('fields', openapi.IN_QUERY,
        #                   description="Selector specifying which fields to include in a partial response.",
        #                   type=openapi.TYPE_STRING),
    ],
        tags=['analytics'])
    def get(self, _request: Request, **_kwargs):
        start_date = self.request.query_params.get(self._START_DATE, self._DEFAULT_START_DATE)
        end_date = self.request.query_params.get(self._END_DATE, self._DEFAULT_END_DATE)
        metrics = self.request.query_params.get(self._METRICS, self._DEFAULT_METRICS)
        dimensions = self.request.query_params.get(self._DIMENSIONS, self._DEFAULT_DIMENSIONS)
        filters = self.request.query_params.get(self._FILTERS, self._DEFAULT_FILTERS)
        include_empty_rows = self.request.query_params.get(self._INCLUDE_EMPTY_ROWS, self._DEFAULT_INCLUDE_EMPTY_ROWS)
        max_results = self.request.query_params.get(self._MAX_RESULTS, self._DEFAULT_MAX_RESULTS)
        output = self.request.query_params.get(self._OUTPUT, self._DEFAULT_OUTPUT)
        samplingLevel = self.request.query_params.get(self._SAMPLINGLEVEL, self._DEFAULT_SAMPLINGLEVEL)
        segment = self.request.query_params.get(self._SEGMENT, self._DEFAULT_SEGMENT)
        sort = self.request.query_params.get(self._SORT, self._DEFAULT_SORT)
        start_index = self.request.query_params.get(self._START_INDEX, self._DEFAULT_START_INDEX)
        # fields = self.request.query_params.get(self._FIELDS, self._DEFAULT_FIELDS)
        return self._get_data(self.google_analytics(start_date, end_date, metrics, dimensions, filters, include_empty_rows, max_results, output, samplingLevel, segment, sort, start_index,
                                                    # fields
                                                    ))

    def _get_data(self) -> Iterable[Dict[str, Any]]:
        data = super()._get_data()
        return self._process(data)

    def devuelve_rows(view_id, select_sql, filter_sql, _page, _pageSize):
        """
        Devuelve Rows
        Args:
            view_id(Integer): ID of the View to query
            select_sql(String): String fields you want to retrieve. If are more than one,separate them by a coma (SQL Format)
            filro_sql(String): String with filters to add to the query (SQL Format)
        Returns:
            resultados(Array): Array with records requested in the query
            columns(Array): Array with the name of the Fields
        """
        try:
            deb("*********************view_id: " + str(view_id))
            deb("*********************select_sql: " + str(select_sql))
            deb("*********************filter_sql: " + str(filter_sql))
            deb("*********************_page: " + str(_page))
            deb("*********************_pageSize: " + str(_pageSize))

            # Query to the View according to environment
            db_v = conexiones.conexion(configuracion.VIEWS_DB).cadena

            cursor_v = db_v.cursor()
            cursor_v.execute(
                "SELECT NOMBREREAL,BASEDATOS from " + configuracion.OPEN_VIEWS + " WHERE ID_VISTA = '" + str(
                    view_id) + "'")
            rows = cursor_v.fetchall()

            # Keep Fields 'NOMBRE' and 'BASEDEDATOS'
            for i in rows:
                deb("--------------------")
                deb("Seleccionamos vista: " + i[0])
                nombre_vista = i[0]
                tipo_vista = i[1]
            cursor_v.close()

            try:
                deb("**tipo_vista: " + str(tipo_vista))
                db = conexiones.conexion(tipo_vista).cadena

                deb("--------------------")
                deb("CADENA DE CONEXION: " + str(db))
                deb("--------------------")

                # Obtain the type of Database (oracle, mysql, postge o sqlserver) according to environment
                tipo = conexiones.conexion(tipo_vista).tipo
                deb("1-TIPO DE BASE DE DATOS: " + str(tipo))
            except Exception as e:
                my_logger.error("View " + str(view_id) + " does not exist - e: " + str(e))
                return json.dumps(["View " + str(view_id) + " does not exist"], ensure_ascii=False, sort_keys=True,
                                  indent=4)

            if _page == '' or _page is None:
                _page = 1
            if _pageSize == '' or _pageSize is None:
                _pageSize = 999999

            url = nombre_vista.replace("'", "")
            parsed = urlparse.urlparse(url)
            profile = str(urlparse.parse_qs(parsed.query)['profile'][0])
            start_date = str(urlparse.parse_qs(parsed.query)['start_date'][0])
            end_date = str(urlparse.parse_qs(parsed.query)['end_date'][0])
            metrics = str(urlparse.parse_qs(parsed.query)['metrics'][0])
            try:
                dimensions = str(urlparse.parse_qs(parsed.query)['dimensions'][0])
            except Exception as e:
                dimensions = None

            try:
                filters = str(urlparse.parse_qs(parsed.query)['filters'][0])
            except Exception as e:
                filters = None

            try:
                include_empty_rows = str(urlparse.parse_qs(parsed.query)['include_empty_rows'][0])
            except Exception as e:
                include_empty_rows = None

            try:
                max_results = str(urlparse.parse_qs(parsed.query)['max_results'][0])
            except Exception as e:
                max_results = None

            try:
                output = str(urlparse.parse_qs(parsed.query)['output'][0])
            except Exception as e:
                output = None

            try:
                samplingLevel = str(urlparse.parse_qs(parsed.query)['samplingLevel'][0])
            except Exception as e:
                samplingLevel = None

            try:
                segment = str(urlparse.parse_qs(parsed.query)['segment'][0])
            except Exception as e:
                segment = None

            try:
                sort = str(urlparse.parse_qs(parsed.query)['sort'][0])
            except Exception as e:
                sort = None

            try:
                start_index = str(urlparse.parse_qs(parsed.query)['start_index'][0])
            except Exception as e:
                start_index = None

            try:
                fields = str(urlparse.parse_qs(parsed.query)['fields'][0])
            except Exception as e:
                fields = None

            rowToStart = (int(_page) * int(_pageSize)) - int(_pageSize) + 1
            rowToEnd = int(_pageSize)
            rowToStart = str(rowToStart)
            rowToEnd = str(rowToEnd)

            if max_results is None or max_results == '' or max_results == '100000':
                max_results = rowToEnd
            if start_index is None or start_index == '':
                start_index = rowToStart
            response = google_analytics(profile, start_date, end_date, metrics, dimensions, filters,
                                        include_empty_rows, max_results, output, samplingLevel, segment, sort,
                                        start_index, fields)
            columns = []
            resultados = json.loads(response)

            for n in range(len(resultados['columnHeaders'])):
                columns.append(resultados['columnHeaders'][n])
            resultados = resultados['rows']
            columns = [column[0] for column in cursor.description]

            return resultados, columns
        except Exception as e:
            my_logger.error(e)
            return [], [" Something went wrong. please try again or contact your administrator"]

    def get_service(self, api_name, api_version, scope, key_file_location,
                    service_account_email):
        """Get a service that communicates to a Google API.
        Args:
          api_name: The name of the api to connect to.
          api_version: The api version to connect to.
          scope: A list auth scopes to authorize for the application.
          key_file_location: The path to a valid service account p12 key file.
          service_account_email: The service account email address.
        Returns:
          A service that is connected to the specified API.
        """

        f = open(key_file_location, 'rb')
        key = f.read()
        f.close()

        # credentials = SignedJwtAssertionCredentials(service_account_email, key, scope=scope)

        credentials = ServiceAccountCredentials.from_p12_keyfile(
            service_account_email=service_account_email,
            filename=key_file_location,
            scopes=scope)

        http = credentials.authorize(httplib2.Http())

        # Build the service object.
        service = build(api_name, api_version, http=http)
        return service

    def google_analytics(self, start_date, end_date, metrics, dimensions, filters, include_empty_rows, max_results,
                         output, samplingLevel, segment, sort, start_index,
                         #fields
                         ):
        """Returns a query to google analytics API.
        https://developers.google.com/apis-explorer/#p/analytics/v3/
        Using:
        https://developers.google.com/apis-explorer/#p/analytics/v3/analytics.data.ga.get
         Args:
            start_date(String): Requests can specify a start date formatted as YYYY-MM-DD, or as a relative date (e.g., today, yesterday, or 7daysAgo). The default value is 7daysAgo. (string)
            end_date(String): Requests can specify a start date formatted as YYYY-MM-DD, or as a relative date (e.g., today, yesterday, or 7daysAgo). The default value is 7daysAgo. (string)
            metrics(String): A comma-separated list of Analytics metrics. E.g., 'ga:sessions,ga:pageviews'. At least one metric must be specified. (string)
            dimensions(String):A comma-separated list of Analytics dimensions. E.g., 'ga:browser,ga:city'.
            filters(String):A comma-separated list of dimension or metric filters to be applied to Analytics data.
            include-empty-rows(Boolean):The response will include empty rows if this parameter is set to true, the default is true
            max-results(Integer):The maximum number of entries to include in this feed.
            output(String):The selected format for the response. Default format is JSON.
            samplingLevel(String):The desired sampling level.
            segment(String):An Analytics segment to be applied to data.
            sort(String):A comma-separated list of dimensions or metrics that determine the sort order for Analytics data.
            start-index(integer, 1+):An index of the first entity to retrieve. Use this parameter as a pagination mechanism along with the max-results parameter.
            fields:Selector specifying which fields to include in a partial response.
        Returns:
            reultados(Dictionary): Array in dictionary format.
        """
        # Use the Analytics Service Object to query the Core Reporting API
        service_account_email = CONFIG.projects.google_analytics.service_account

        # key_file_location = 'client_secrets.p12'
        key_file_location = CONFIG.projects.google_analytics.key_file_location

        # Define the auth scopes to request.
        scope = ['https://www.googleapis.com/auth/analytics.readonly']
        service = self.get_service('analytics', 'v3', scope, key_file_location, service_account_email)

        resultado = service.data().ga().get(
            ids='ga:' + self._FIELD_TAGS,
            start_date=start_date,
            end_date=end_date,
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            include_empty_rows=include_empty_rows,
            max_results=max_results,
            output=output,
            samplingLevel=samplingLevel,
            segment=segment,
            sort=sort,
            start_index=start_index,
            # fields=fields
        ).execute()

        # Return  results in JSON format indented.
        return json.dumps(resultado, ensure_ascii=False, sort_keys=True, indent=1)





