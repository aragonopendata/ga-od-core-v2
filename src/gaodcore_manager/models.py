from django.db import models


class ConnectorConfig(models.Model):
    id = models.AutoField(primary_key=True, help_text="Primary key of ConnectorConfig.")
    name = models.CharField(max_length=255, unique=True,
                            help_text="Human readable name. This must be identificable and unique.")
    uri = models.TextField(
        unique=True,
        help_text='This can have and http/s API or database URI. Ex. "https://domain.es/file.ext" or '
                  '"oracle://username:password@host".')
    enabled = models.BooleanField(help_text="Resource will be offered if conector and resource are enabled.")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return str(self.name)


class ResourceConfig(models.Model):
    id = models.AutoField(primary_key=True, help_text="Primary key of ConnectorConfig.")
    name = models.CharField(
        max_length=255, unique=True,
        help_text="Human readable name. This must be identificable and unique.")
    connector_config = models.ForeignKey(ConnectorConfig, on_delete=models.CASCADE,
                                         help_text="Foreign key of ConnectorConfig.")

    enabled = models.BooleanField(
        help_text="Resource will be offered if ConectorConfig and ResourceConfig are enabled.")
    object_location = models.CharField(
        max_length=255,
        null=True,
        help_text="Only used in database resources. it can be a table, view, fucntion, etc. APIs resources "
                  "must be null.")
    object_location_schema = models.CharField(
        max_length=255,
        null=True,
        help_text="Only used in database resources. Is not required if are in default schema. APIs resources must be "
                  "null.")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return str(self.name)
