from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.db.models import Count
from django.db.models.functions import Lower
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.utils.translation import gettext_lazy as _
from django.views.generic import DetailView, ListView
from django.views.generic.edit import CreateView, DeleteView, UpdateView

from gaodcore_manager.forms import ConnectorConfigForm, ResourceConfigForm
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
        # Drives the shared private administration shell's global navigation
        # (see gaodcore_manager/private_base.html). Manager's section values
        # ("resources"/"connectors") line up with the shared nav keys.
        context["private_active_section"] = self.active_section
        return context


class SearchFilterListMixin:
    """Adds a `?q=` name search and an `?enabled=` filter to a ListView, and
    exposes the current values plus the querystring needed to keep them
    across pagination links.

    The `?enabled=` filter defaults to only enabled rows; `?enabled=all`
    removes the filter."""

    search_field = "name"
    default_enabled_filter = "1"

    def get_enabled_filter(self):
        """Returns the requested `enabled` value, defaulting to enabled-only."""
        return self.request.GET.get("enabled", self.default_enabled_filter)

    def filter_queryset(self, queryset):
        query = self.request.GET.get("q", "").strip()
        if query:
            queryset = queryset.filter(**{f"{self.search_field}__icontains": query})
        enabled = self.get_enabled_filter()
        if enabled in ("1", "0"):
            queryset = queryset.filter(enabled=(enabled == "1"))
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["search_query"] = self.request.GET.get("q", "")
        context["enabled_filter"] = self.get_enabled_filter()
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

    def get_connector_filter(self):
        """Returns the requested `connector` value, or "" for all connectors."""
        return self.request.GET.get("connector", "").strip()

    def get_queryset(self):
        queryset = ResourceConfig.objects.select_related("connector_config").order_by("id")
        queryset = self.filter_queryset(queryset)
        connector_filter = self.get_connector_filter()
        # A malformed value (not a plain positive integer) is treated as no
        # filter applied, so it can never reach the database as an invalid
        # connector_config_id lookup.
        if connector_filter.isdigit():
            queryset = queryset.filter(connector_config_id=connector_filter)
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["breadcrumb_section"] = _("Resources")
        context["connector_filter"] = self.get_connector_filter()
        context["connector_choices"] = ConnectorConfig.objects.order_by(Lower("name"), "pk")
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
        # `resource_count` feeds the cascade warning of the deletion modal; a single
        # aggregate avoids one COUNT query per row.
        queryset = ConnectorConfig.objects.annotate(
            resource_count=Count("resourceconfig")
        ).order_by("id")
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
        # Feeds the cascade warning of the deletion modal.
        context["resource_count"] = self.object.resourceconfig_set.count()
        return context


class ManagerFormViewMixin(StaffManagerTemplateMixin):
    """Shared plumbing for the manager create and update forms.

    On success it adds a translatable message naming the object and redirects to
    the object detail, where the destination page renders the notification.
    """

    template_name = "gaodcore_manager/config_form.html"
    success_message = None
    detail_url_name = None
    list_url_name = None
    form_title = None

    def form_valid(self, form):
        response = super().form_valid(form)
        messages.success(self.request, self.success_message % {"name": self.object.name})
        return response

    def get_success_url(self):
        return reverse(self.detail_url_name, kwargs={"pk": self.object.pk})

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_title"] = self.form_title
        context["cancel_url"] = (
            reverse(self.detail_url_name, kwargs={"pk": self.object.pk})
            if self.object is not None
            else reverse(self.list_url_name)
        )
        return context


class ManagerDeleteViewMixin(StaffManagerTemplateMixin):
    """Deletion is only reachable through a CSRF protected POST.

    The confirmation happens in the list/detail modal, so there is no
    intermediate GET confirmation page.
    """

    http_method_names = ["post"]
    success_message = None
    list_url_name = None

    def form_valid(self, form):
        name = self.object.name
        response = super().form_valid(form)
        messages.success(self.request, self.success_message % {"name": name})
        return response

    def get_success_url(self):
        return reverse(self.list_url_name)


class ResourceConfigCreateView(ManagerFormViewMixin, CreateView):
    model = ResourceConfig
    form_class = ResourceConfigForm
    active_section = "resources"
    detail_url_name = "manager_web:resource-detail"
    list_url_name = "manager_web:resource-list"
    form_title = _("New resource")
    success_message = _('The resource "%(name)s" has been created successfully.')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["breadcrumb_section"] = _("Resources")
        context["breadcrumb_section_url"] = reverse("manager_web:resource-list")
        context["breadcrumb_object"] = self.form_title
        return context


class ResourceConfigUpdateView(ManagerFormViewMixin, UpdateView):
    model = ResourceConfig
    form_class = ResourceConfigForm
    active_section = "resources"
    detail_url_name = "manager_web:resource-detail"
    list_url_name = "manager_web:resource-list"
    form_title = _("Edit resource")
    success_message = _('The resource "%(name)s" has been updated successfully.')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["breadcrumb_section"] = _("Resources")
        context["breadcrumb_section_url"] = reverse("manager_web:resource-list")
        context["breadcrumb_object"] = self.object.name
        return context


class ResourceConfigDeleteView(ManagerDeleteViewMixin, DeleteView):
    model = ResourceConfig
    active_section = "resources"
    list_url_name = "manager_web:resource-list"
    success_message = _('The resource "%(name)s" has been deleted successfully.')


class ConnectorConfigCreateView(ManagerFormViewMixin, CreateView):
    model = ConnectorConfig
    form_class = ConnectorConfigForm
    active_section = "connectors"
    detail_url_name = "manager_web:connector-detail"
    list_url_name = "manager_web:connector-list"
    form_title = _("New connector")
    success_message = _('The connector "%(name)s" has been created successfully.')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["breadcrumb_section"] = _("Connectors")
        context["breadcrumb_section_url"] = reverse("manager_web:connector-list")
        context["breadcrumb_object"] = self.form_title
        return context


class ConnectorConfigUpdateView(ManagerFormViewMixin, UpdateView):
    model = ConnectorConfig
    form_class = ConnectorConfigForm
    active_section = "connectors"
    detail_url_name = "manager_web:connector-detail"
    list_url_name = "manager_web:connector-list"
    form_title = _("Edit connector")
    success_message = _('The connector "%(name)s" has been updated successfully.')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["breadcrumb_section"] = _("Connectors")
        context["breadcrumb_section_url"] = reverse("manager_web:connector-list")
        context["breadcrumb_object"] = self.object.name
        return context


class ConnectorConfigDeleteView(ManagerDeleteViewMixin, DeleteView):
    model = ConnectorConfig
    active_section = "connectors"
    list_url_name = "manager_web:connector-list"
    # Deleting a connector cascades to its resources; the web UI warns about it in
    # the confirmation modal.
    success_message = _('The connector "%(name)s" has been deleted successfully.')
