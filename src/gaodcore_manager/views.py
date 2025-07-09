from django.utils.decorators import method_decorator
from django.views.generic import ListView, DetailView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.views import LoginView
from django.db.models import Q
from drf_excel.mixins import XLSXFileMixin
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


# Template-based views for admin interface
class ConnectorListView(LoginRequiredMixin, ListView):
    model = ConnectorConfig
    template_name = 'gaodcore_manager/connector_list.html'
    context_object_name = 'connectors'
    paginate_by = 20
    ordering = ['id']

    def get_queryset(self):
        queryset = super().get_queryset()
        search_query = self.request.GET.get('search')
        status_filter = self.request.GET.get('status')

        if search_query:
            # Try to convert search_query to int for ID search
            try:
                search_id = int(search_query)
                queryset = queryset.filter(
                    Q(id=search_id) |
                    Q(name__icontains=search_query) |
                    Q(uri__icontains=search_query)
                )
            except ValueError:
                # If not a number, search by name and URI only
                queryset = queryset.filter(
                    Q(name__icontains=search_query) |
                    Q(uri__icontains=search_query)
                )

        if status_filter == 'enabled':
            queryset = queryset.filter(enabled=True)
        elif status_filter == 'disabled':
            queryset = queryset.filter(enabled=False)

        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['search_query'] = self.request.GET.get('search', '')
        context['status_filter'] = self.request.GET.get('status', '')
        return context


class ConnectorDetailView(LoginRequiredMixin, DetailView):
    model = ConnectorConfig
    template_name = 'gaodcore_manager/connector_detail.html'
    context_object_name = 'connector'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['resources'] = ResourceConfig.objects.filter(
            connector_config=self.object
        ).order_by('name')
        return context


class ResourceListView(LoginRequiredMixin, ListView):
    model = ResourceConfig
    template_name = 'gaodcore_manager/resource_list.html'
    context_object_name = 'resources'
    paginate_by = 20
    ordering = ['id']

    def get_queryset(self):
        # Only show resources from enabled connectors
        queryset = super().get_queryset().select_related('connector_config').filter(
            connector_config__enabled=True
        )
        search_query = self.request.GET.get('search')
        status_filter = self.request.GET.get('status')
        connector_filter = self.request.GET.get('connector')

        if search_query:
            # Try to convert search_query to int for ID search
            try:
                search_id = int(search_query)
                queryset = queryset.filter(
                    Q(id=search_id) |
                    Q(name__icontains=search_query) |
                    Q(object_location__icontains=search_query) |
                    Q(connector_config__name__icontains=search_query)
                )
            except ValueError:
                # If not a valid integer, search by text fields only
                queryset = queryset.filter(
                    Q(name__icontains=search_query) |
                    Q(object_location__icontains=search_query) |
                    Q(connector_config__name__icontains=search_query)
                )

        if status_filter == 'enabled':
            queryset = queryset.filter(enabled=True)
        elif status_filter == 'disabled':
            queryset = queryset.filter(enabled=False)

        if connector_filter:
            queryset = queryset.filter(connector_config__id=connector_filter)

        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['search_query'] = self.request.GET.get('search', '')
        context['status_filter'] = self.request.GET.get('status', '')
        context['connector_filter'] = self.request.GET.get('connector', '')
        # Only include enabled connectors in the filter dropdown
        context['connectors'] = ConnectorConfig.objects.filter(enabled=True).order_by('name')
        return context


class ResourceDetailView(LoginRequiredMixin, DetailView):
    model = ResourceConfig
    template_name = 'gaodcore_manager/resource_detail.html'
    context_object_name = 'resource'


class ManagerLoginView(LoginView):
    template_name = 'gaodcore_manager/login.html'
    redirect_authenticated_user = True

    def get_success_url(self):
        return '/GA_OD_Core_admin/manager/views/resources/'
