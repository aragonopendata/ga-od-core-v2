from django.utils.decorators import method_decorator
from drf_excel.mixins import XLSXFileMixin
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
from rest_framework import viewsets
from rest_framework.parsers import FormParser, JSONParser, MultiPartParser
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from gaodcore_manager.serializers import (
    ConnectorConfigSerializer,
    ResourceConfigSerializer,
    ValidatorRequestSerializer,
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
    permission_classes = (IsAdminUser,)


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
    permission_classes = (IsAdminUser,)


class ValidatorView(APIViewMixin):
    permission_classes = (IsAdminUser,)
    # The global DEFAULT_PARSER_CLASSES only contains FormParser and
    # MultiPartParser (see settings.py), so JSONParser must be added explicitly
    # here for the POST endpoint to accept a JSON body. FormParser is kept
    # first (matching the global default order) so the browsable API's parser
    # introspection behaves the same as it did before POST existed.
    parser_classes = [FormParser, MultiPartParser, JSONParser]

    def get_serializer(self, *args, **kwargs):
        """Keep APIViewMixin.get_serializer's DictSerializer for rendering the GET
        response, but not for BrowsableAPIRenderer's POST/PUT/PATCH form preview.

        Adding ``post`` makes the browsable API try to build an HTML preview form
        for POST too. That preview asks for a serializer representing the
        *input*, not the output DictSerializer (built from ``self.response.data``,
        which holds output rows - it is not meant to represent a write payload
        and was never previously exercised in HTML rendering since only GET
        existed). ``ValidatorRequestSerializer`` is the actual input contract for
        POST, so it renders a correct, harmless empty form instead.
        """
        if not getattr(self, "swagger_fake_view", False) and self.request.method != "GET":
            return ValidatorRequestSerializer()
        return super().get_serializer(*args, **kwargs)

    @staticmethod
    def _validate_and_respond(
        request, uri, object_location, object_location_schema
    ) -> Response:
        """Shared logic for GET and POST: validate the resource and build the response.

        Kept as a single helper so the GET (query params) and POST (body) paths
        cannot drift from each other.
        """
        data = resource_validator(
            uri=uri,
            object_location=object_location,
            object_location_schema=object_location_schema,
        )

        # Check if the request is for XLSX format
        accept_header = request.META.get('HTTP_ACCEPT', '')
        format_is_xlsx = 'application/xlsx' in accept_header or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in accept_header

        return Response(get_return_list(data, format_is_xlsx=format_is_xlsx))

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
    def get(self, request, **_kwargs) -> Response:
        uri = request.query_params.get("uri")
        object_location = request.query_params.get("object_location")
        object_location_schema = request.query_params.get("object_location_schema")
        return self._validate_and_respond(
            request, uri, object_location, object_location_schema
        )

    @extend_schema(tags=["manager"], request=ValidatorRequestSerializer)
    def post(self, request, **_kwargs) -> Response:
        """Same validation as GET, but the URI travels in the body instead of the URL.

        A body is not part of the URL and is not logged by
        ``APIViewMixin.initial()``, so it keeps the URI (and its password) out
        of application logs, the request line and proxy/browser history.
        """
        body_serializer = ValidatorRequestSerializer(data=request.data)
        body_serializer.is_valid(raise_exception=True)
        validated = body_serializer.validated_data
        return self._validate_and_respond(
            request,
            validated["uri"],
            validated.get("object_location"),
            validated.get("object_location_schema"),
        )
