from django.db import models


class ConnectorConfig(models.Model):
    """
    Model representing the configuration for a connection to a database or api.

    @param id: AutoField - Primary key of ConnectorConfig.
    @param name: CharField - Human readable name. This must be identifiable and unique.
    @param uri: TextField - This can have an HTTP/S API or database URI. Example: "https://domain.es/file.ext" or
                 "oracle://username:password@host".
    @param enabled: BooleanField - Resource will be offered if connector and resource are enabled.
    @param created_at: DateTimeField - Timestamp when the record was created.
    @param updated_at: DateTimeField - Timestamp when the record was last updated.
    """
    id = models.AutoField(primary_key=True, help_text="Primary key of ConnectorConfig.")
    name = models.CharField(max_length=255,
                            unique=True,
                            help_text="Human readable name. This must be identificable and unique.")
    uri = models.TextField(
        unique=True,
        help_text='This can have and http/s API or database URI. Ex. "https://domain.es/file.ext" or '
        '"oracle://username:password@host".')
    enabled = models.BooleanField(help_text="Resource will be offered if connector and resource are enabled.")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return str(self.name)


class ResourceConfig(models.Model):
    """
      Model representing the configuration for a resource.

      @param id: AutoField - Primary key of ResourceConfig.
      @param name: CharField - Human readable name. This must be identifiable and unique.
      @param connector_config: ForeignKey - Foreign key to ConnectorConfig.
      @param enabled: BooleanField - Indicates whether the resource will be offered if both the connector and resource are enabled.
      @param object_location: CharField - Only used in database resources. It can be a table, view, function, etc. API resources must be null.
      @param object_location_schema: CharField - Only used in database resources. Not required if in the default schema. API resources must be null.
      @param created_at: DateTimeField - Timestamp when the record was created.
      @param updated_at: DateTimeField - Timestamp when the record was last updated.
      """
    id = models.AutoField(primary_key=True, help_text="Primary key of ResourceConfig.")
    name = models.CharField(max_length=255,
                            unique=True,
                            help_text="Human readable name. This must be identificable and unique.")
    connector_config = models.ForeignKey(ConnectorConfig,
                                         on_delete=models.CASCADE,
                                         help_text="Foreign key of ResourceConfig.")

    enabled = models.BooleanField(
        help_text="Resource will be offered if ConnectorConfig and ResourceConfig are enabled.")
    object_location = models.CharField(
        max_length=255,
        null=True,
        help_text="Only used in database resources. it can be a table, view, function, etc. APIs resources "
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
    

class ResourceSizeConfig(models.Model):
    resource_id = models.OneToOneField(ResourceConfig,
                                       primary_key=True,
                                       on_delete=models.CASCADE,
                                       help_text="Foreign key of ResourceConfig.")

    registries = models.BigIntegerField(
        null=True,
        help_text="Number of resgistries of the resource"
        )
    size = models.BigIntegerField(
        null=True,
        help_text="Size in Mb of the resource "
        )
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return str(self.resource_id)