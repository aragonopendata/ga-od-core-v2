import json
import logging

from django.core.validators import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

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
    table = models.CharField(max_length=100)
    connect_args = models.TextField(default='{}', validators=[validate_connect_args])
    sqla_string = models.CharField(max_length=500, null=True)

    def clean(self):
        error = None
        engine = create_engine(self.sqla_string, connect_args=json.loads(self.connect_args))
        try:
            if not engine.dialect.has_table(engine, self.table):
                error = ValidationError(
                    _('Table %(table)s does not exist.'),
                    params={'table': self.table}
                )
        except SQLAlchemyError:
            error = ValidationError(
                _('Error establishing connection with string %(sqla_string)s.'),
                params={'sqla_string': self.sqla_string}
            )
        finally:
            if error is not None:
                raise error

    def save(self, *args, **kwargs):
        self.sqla_string = '{db_type}://{username}:{password}@{host}:{port}/{database}'.format(**vars(self))
        self.full_clean()
        super(ConexionDB, self).save(*args, **kwargs)
