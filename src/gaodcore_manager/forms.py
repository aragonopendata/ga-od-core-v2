"""Django forms for the manager web CRUD.

The business rules live in `gaodcore_manager.validators` and are shared with the
REST serializers. The only thing that is specific to this interface is the
translation of the DRF exceptions those validators raise into Django form
errors.
"""

from contextlib import contextmanager

from django import forms
from rest_framework.exceptions import APIException

from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from gaodcore_manager.validators import (
    resource_persistence_validator,
    uri_persistence_validator,
)


def _flatten_detail(detail):
    """Turns a DRF error detail (string, list or dict) into a list of strings."""
    if isinstance(detail, dict):
        return [
            message
            for value in detail.values()
            for message in _flatten_detail(value)
        ]
    if isinstance(detail, (list, tuple)):
        return [message for value in detail for message in _flatten_detail(value)]
    return [str(detail)]


@contextmanager
def api_errors_as_form_errors():
    """Re-raises the DRF errors of the shared validators as form errors.

    `ValidationError` and `ServiceUnavailable` are both `APIException`
    subclasses and cannot travel through a `ModelForm` unchanged.
    """
    try:
        yield
    except APIException as err:
        raise forms.ValidationError(_flatten_detail(err.detail)) from err


class ConnectorConfigForm(forms.ModelForm):
    class Meta:
        model = ConnectorConfig
        fields = ["name", "uri", "enabled"]
        widgets = {
            "uri": forms.Textarea(
                attrs={"rows": 3, "class": "manager-form__uri-field"}
            )
        }

    def clean_uri(self):
        uri = self.cleaned_data["uri"]
        # On update, only a changed URI justifies an external probe.
        if self.instance.pk and self.instance.uri == uri:
            return uri
        with api_errors_as_form_errors():
            uri_persistence_validator(uri)
        return uri


class ResourceConfigForm(forms.ModelForm):
    class Meta:
        model = ResourceConfig
        fields = [
            "name",
            "connector_config",
            "enabled",
            "object_location",
            "object_location_schema",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # The model allows NULL on both columns; the REST API stores None rather
        # than an empty string, so the form must be optional and normalize to None.
        for field_name in ("object_location", "object_location_schema"):
            self.fields[field_name].required = False
        self.fields["connector_config"].queryset = ConnectorConfig.objects.order_by(
            "name"
        )

    def clean_object_location(self):
        return self.cleaned_data.get("object_location") or None

    def clean_object_location_schema(self):
        return self.cleaned_data.get("object_location_schema") or None

    def clean(self):
        cleaned_data = super().clean()
        connector_config = cleaned_data.get("connector_config")
        if connector_config is None:
            # The field error is already reported; there is nothing to validate.
            return cleaned_data

        object_location = cleaned_data.get("object_location")
        object_location_schema = cleaned_data.get("object_location_schema")

        if self.instance.pk and not self._connection_fields_changed(
            connector_config, object_location, object_location_schema
        ):
            # Metadata only update: nothing that defines the connection changed.
            return cleaned_data

        with api_errors_as_form_errors():
            resource_persistence_validator(
                uri=connector_config.uri,
                object_location=object_location,
                object_location_schema=object_location_schema,
            )
        return cleaned_data

    def _connection_fields_changed(
        self, connector_config, object_location, object_location_schema
    ) -> bool:
        # `clean()` runs before `_post_clean()`, so `self.instance` still holds the
        # stored values here.
        stored = self.instance
        return (
            connector_config != stored.connector_config
            or object_location != stored.object_location
            or object_location_schema != stored.object_location_schema
        )
