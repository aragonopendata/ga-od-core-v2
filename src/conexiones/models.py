import json
import logging

from django.core.validators import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def validate_connect_args(value: str):
    if value is not None:
        try:
            json.loads(value)
        except json.JSONDecodeError:
            raise ValidationError(
                _('%(value)s is not a valid JSON'),
                params={'value': value}
            )


class ConexionDB(models.Model):
    DB_TYPE_CHOICES = (
        ('postgresql+psycopg2', 'PostgreSQL'),
        ('mysql+mysqlconnector', 'MySQL'),
        ('oracle+cx_oracle', 'Oracle'),
    )

    id = models.AutoField(primary_key=True)
    host = models.CharField(max_length=100)
    port = models.PositiveIntegerField()
    db_type = models.CharField(max_length=100, choices=DB_TYPE_CHOICES)
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=100)
    database = models.CharField(max_length=100)
    sqla_string = models.CharField(max_length=500, null=True)
    connect_args = models.TextField(default='{}', validators=[validate_connect_args])

    def clean(self):
        engine = create_engine(self.sqla_string, connect_args=json.loads(self.connect_args))
        try:
            with engine.connect() as connection:
                r = connection.execute('select 1 as is_alive;')
        except Exception:
            raise ValidationError(
                _('Error establishing connection with string %(sqla_string)s.'),
                params={'sqla_string': self.sqla_string}
            )

    def save(self, *args, **kwargs):
        self.sqla_string = '{db_type}://{username}:{password}@{host}:{port}/{database}'.format(**vars(self))
        self.full_clean()
        super(ConexionDB, self).save(*args, **kwargs)
