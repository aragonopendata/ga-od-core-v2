from django.db import models


# Create your models here.
class ConnectorConfig(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255, unique=True)
    uri = models.TextField(unique=True)
    enabled = models.BooleanField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name


class ResourceConfig(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255, unique=True)
    connector_config = models.ForeignKey(ConnectorConfig, on_delete=models.CASCADE)

    enabled = models.BooleanField()
    object_location = models.CharField(max_length=255, null=True)
    object_location_schema = models.CharField(max_length=255, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name
