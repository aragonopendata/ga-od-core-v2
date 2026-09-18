from django.contrib.admin.views.decorators import staff_member_required
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.utils.translation import gettext_lazy as _
from django.views.generic import DetailView, ListView

from gaodcore_manager.models import ConnectorConfig, ResourceConfig


class StaffManagerTemplateMixin:
    """Restricts a view to active staff users and adds manager navigation context."""

    active_section = None

    @method_decorator(staff_member_required)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["active_section"] = self.active_section
        return context


class SearchFilterListMixin:
    """Adds a `?q=` name search and an `?enabled=` filter to a ListView, and
    exposes the current values plus the querystring needed to keep them
    across pagination links."""

    search_field = "name"

    def filter_queryset(self, queryset):
        query = self.request.GET.get("q", "").strip()
        if query:
            queryset = queryset.filter(**{f"{self.search_field}__icontains": query})
        enabled = self.request.GET.get("enabled", "")
        if enabled in ("1", "0"):
            queryset = queryset.filter(enabled=(enabled == "1"))
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["search_query"] = self.request.GET.get("q", "")
        context["enabled_filter"] = self.request.GET.get("enabled", "")
        pagination_params = self.request.GET.copy()
        pagination_params.pop("page", None)
        context["pagination_query_string"] = pagination_params.urlencode()
        return context


class ResourceConfigListView(SearchFilterListMixin, StaffManagerTemplateMixin, ListView):
    model = ResourceConfig
    template_name = "gaodcore_manager/resourceconfig_list.html"
    context_object_name = "resources"
    active_section = "resources"
    paginate_by = 50

    def get_queryset(self):
        queryset = ResourceConfig.objects.select_related("connector_config").order_by("id")
        return self.filter_queryset(queryset)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["breadcrumb_section"] = _("Resources")
        return context


class ResourceConfigDetailView(StaffManagerTemplateMixin, DetailView):
    model = ResourceConfig
    template_name = "gaodcore_manager/resourceconfig_detail.html"
    context_object_name = "resource"
    active_section = "resources"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["breadcrumb_section"] = _("Resources")
        context["breadcrumb_section_url"] = reverse("manager_web:resource-list")
        context["breadcrumb_object"] = self.object.name
        return context


class ConnectorConfigListView(SearchFilterListMixin, StaffManagerTemplateMixin, ListView):
    model = ConnectorConfig
    template_name = "gaodcore_manager/connectorconfig_list.html"
    context_object_name = "connectors"
    active_section = "connectors"
    paginate_by = 50

    def get_queryset(self):
        queryset = ConnectorConfig.objects.order_by("id")
        return self.filter_queryset(queryset)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["breadcrumb_section"] = _("Connectors")
        return context


class ConnectorConfigDetailView(StaffManagerTemplateMixin, DetailView):
    model = ConnectorConfig
    template_name = "gaodcore_manager/connectorconfig_detail.html"
    context_object_name = "connector"
    active_section = "connectors"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["breadcrumb_section"] = _("Connectors")
        context["breadcrumb_section_url"] = reverse("manager_web:connector-list")
        context["breadcrumb_object"] = self.object.name
        return context
