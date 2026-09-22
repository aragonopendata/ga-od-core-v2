import logging

from django.contrib import messages
from django.db.models import CharField, Count, Q
from django.db.models.functions import Cast, Lower
from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.utils.translation import gettext_lazy as _
from django.views import View
from django.views.generic import DetailView, ListView
from django.views.generic.edit import CreateView, DeleteView, UpdateView
from rest_framework.exceptions import APIException

from exceptions import ErrorCodes
from gaodcore_manager import validators
from gaodcore_manager.auth import staff_required
from gaodcore_manager.forms import ConnectorConfigForm, ResourceConfigForm
from gaodcore_manager.models import ConnectorConfig, ResourceConfig

logger = logging.getLogger(__name__)


class StaffManagerTemplateMixin:
    """Restricts a view to active staff users and adds manager navigation context."""

    active_section = None

    @method_decorator(staff_required)
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
    """Adds a `?q=` search across `search_fields` (plus the row id) and an
    `?enabled=` filter to a ListView, and exposes the current values plus the
    querystring needed to keep them across pagination links.

    The `?enabled=` filter defaults to only enabled rows; `?enabled=all`
    removes the filter."""

    search_fields = ("name",)
    default_enabled_filter = "1"

    def get_enabled_filter(self):
        """Returns the requested `enabled` value, defaulting to enabled-only."""
        return self.request.GET.get("enabled", self.default_enabled_filter)

    def filter_queryset(self, queryset):
        query = self.request.GET.get("q", "").strip()
        if query:
            queryset = queryset.annotate(id_as_text=Cast("id", output_field=CharField()))
            search_query = Q(id_as_text__icontains=query)
            for field in self.search_fields:
                search_query |= Q(**{f"{field}__icontains": query})
            queryset = queryset.filter(search_query)
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
    search_fields = ("name", "connector_config__name")

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


class ManagerCheckViewMixin(StaffManagerTemplateMixin):
    """Shared plumbing for the manual "check" actions of the private manager.

    The check always performs real external I/O: unlike the automatic probe run
    while saving, it is an explicit user action, so
    `GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS` does not apply here. Disabled
    objects can be checked too.

    Nothing is persisted; the outcome is reported through the messages
    framework and the request always ends in a redirect back to the detail page
    (POST/Redirect/GET).
    """

    http_method_names = ["post"]
    model = None
    detail_url_name = None
    success_message = None
    #: Maps a known `ErrorCodes` value to the user facing message of this view.
    error_messages = {}
    #: Used for an APIException whose code is unknown or not a single string.
    generic_error_message = None
    unexpected_error_message = None
    #: Discriminates connector from resource checks in the log records.
    check_kind = None

    def run_check(self, obj):
        """Perform the external check, raising APIException on a known failure."""
        raise NotImplementedError

    def post(self, request, *args, **kwargs):
        obj = get_object_or_404(self.model, pk=kwargs["pk"])
        try:
            self.run_check(obj)
        except APIException as exc:
            message = self.error_messages.get(
                _api_exception_code(exc), self.generic_error_message
            )
            messages.error(request, message % {"name": obj.name})
        except Exception as exc:  # pylint: disable=broad-except
            # Only non-sensitive identifiers are logged: the connectors layer
            # already emits its own diagnostics, and neither the URI, the
            # credentials nor the raw exception text may reach the logs here.
            logger.error(
                "Unexpected %s check failure: object_id=%s error_type=%s",
                self.check_kind,
                obj.pk,
                type(exc).__name__,
            )
            messages.error(request, self.unexpected_error_message % {"name": obj.name})
        else:
            messages.success(request, self.success_message % {"name": obj.name})
        return HttpResponseRedirect(reverse(self.detail_url_name, kwargs={"pk": obj.pk}))


def _api_exception_code(exc):
    """Return the single error code of `exc`, or None when there is not exactly one.

    `ValidationError` wraps its detail in a list, so `get_codes()` returns a
    one-element list for the validator failures; `ServiceUnavailable` returns a
    plain string. Anything else (a dict of per-field codes, several codes) has
    no single stable code and falls back to the generic message.
    """
    codes = exc.get_codes()
    if isinstance(codes, (list, tuple)) and len(codes) == 1:
        codes = codes[0]
    return codes if isinstance(codes, str) else None


class ConnectorConfigCheckView(ManagerCheckViewMixin, View):
    model = ConnectorConfig
    active_section = "connectors"
    detail_url_name = "manager_web:connector-detail"
    check_kind = "connector"
    success_message = _('The connector "%(name)s" is available.')
    generic_error_message = _('The connection check for connector "%(name)s" failed.')
    unexpected_error_message = _(
        'An unexpected error occurred while checking connector "%(name)s". '
        "Consult the application logs."
    )
    error_messages = {
        ErrorCodes.CONNECTION_UNAVAILABLE: _(
            'Could not connect to connector "%(name)s". '
            "Check its credentials, server, and port."
        ),
        ErrorCodes.SCHEMA_NOT_IMPLEMENTED: _(
            'The connection type configured for "%(name)s" is not supported.'
        ),
        ErrorCodes.MIME_TYPE_NOT_ALLOWED: _(
            'The HTTP origin configured for "%(name)s" did not return an allowed '
            "JSON content type."
        ),
    }

    def run_check(self, obj):
        validators.uri_validator(obj.uri)


class ResourceConfigCheckView(ManagerCheckViewMixin, View):
    model = ResourceConfig
    active_section = "resources"
    detail_url_name = "manager_web:resource-detail"
    check_kind = "resource"
    success_message = _('The resource "%(name)s" is available and can be queried.')
    generic_error_message = _('The check for resource "%(name)s" failed.')
    unexpected_error_message = _(
        'An unexpected error occurred while checking resource "%(name)s". '
        "Consult the application logs."
    )
    error_messages = {
        ErrorCodes.CONNECTION_UNAVAILABLE: _(
            'Could not connect to the connector used by resource "%(name)s". '
            "Check the connector configuration."
        ),
        ErrorCodes.RESOURCE_UNAVAILABLE: _(
            "The connection is available, but the object configured for resource "
            '"%(name)s" could not be accessed.'
        ),
        ErrorCodes.SCHEMA_NOT_IMPLEMENTED: _(
            'The connection type configured for "%(name)s" is not supported.'
        ),
        ErrorCodes.MIME_TYPE_NOT_ALLOWED: _(
            'The HTTP origin configured for "%(name)s" did not return an allowed '
            "JSON content type."
        ),
        ErrorCodes.TOO_MANY_ROWS: _(
            'Resource "%(name)s" responds, but it exceeds the allowed row limit.'
        ),
        ErrorCodes.REQUIRED: _(
            'Resource "%(name)s" has an invalid configuration. '
            "Review its object location and schema."
        ),
        ErrorCodes.INVALID_FIELD: _(
            'Resource "%(name)s" has an invalid configuration. '
            "Review its object location and schema."
        ),
    }

    def run_check(self, obj):
        result = validators.resource_validator(
            obj.connector_config.uri,
            obj.object_location,
            obj.object_location_schema,
            # Proving the resource is queryable needs one row at most; without
            # this the shared validator would materialize the whole table.
            limit=1,
        )
        # Some origins only fail while the rows are produced, so the lazy
        # iterable must be consumed. The rows themselves are never rendered.
        list(result)
