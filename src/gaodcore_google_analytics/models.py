from django.db import models

# Create your models here.
class Google_Analytics(models.Model):
    # Para la llamada a la API
    api_name = models.CharField(max_length = 250)
    api_version = models.CharField(max_length = 250)
    # scope = models.List()
    key_file_location =  models.CharField(max_length = 250) # path to p12 file
    service_account_email = models.CharField(max_length = 250)

    def __str__(self):
        return self