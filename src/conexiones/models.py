import json
import logging

import requests
from django.core.validators import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _
from requests.exceptions import RequestException
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

# TODO: Add ERROR and DEBUG logs

logger = logging.getLogger(__name__)


def validate_json(value: str):
    if value is not None:
        try:
            json.loads(value)
        except json.JSONDecodeError:
            raise ValidationError(
                _('%(value)s is not a valid JSON'),
                params={'value': value}
            )


class Conexion(models.Model):
    CONEXION_TYPE_CHOICES = (
        ('DB', 'DB'),
        ('API', 'API')
    )
    id = models.AutoField(primary_key=True)
    nombre = models.CharField(max_length=100, unique=True)
    conexion_type = models.CharField(max_length=10, choices=CONEXION_TYPE_CHOICES)
    host = models.CharField(max_length=100)
    port = models.PositiveIntegerField()


class ConexionDB(Conexion):
    DB_TYPE_CHOICES = (
        ('postgresql+psycopg2', 'PostgreSQL'),
        ('mysql+mysqlconnector', 'MySQL'),
        ('oracle+cx_oracle', 'Oracle'),
    )

    db_type = models.CharField(max_length=100, choices=DB_TYPE_CHOICES)
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=100)
    database = models.CharField(max_length=100)
    table = models.CharField(max_length=100)
    connect_args = models.TextField(default='{}', validators=[validate_json])
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
        self.conexion_type = 'DB'
        self.sqla_string = '{db_type}://{username}:{password}@{host}:{port}/{database}'.format(**vars(self))
        self.full_clean()
        super(ConexionDB, self).save(*args, **kwargs)


class ConexionAPI(Conexion):
    REST_OPERATION_CHOICES = (
        ('GET', 'GET'),
        ('POST', 'POST'),
        ('PUT', 'PUT'),
        ('DELETE', 'DELETE'),
    )

    endpoint = models.CharField(max_length=200)
    url = models.CharField(max_length=500)
    method = models.CharField(max_length=10, choices=REST_OPERATION_CHOICES)
    params = models.TextField(default='{}', validators=[validate_json])
    headers = models.TextField(default='{"Content-Type": "application/json"}', validators=[validate_json])
    data = models.TextField(default='{}', validators=[validate_json])
    username = models.CharField(max_length=100, blank=True)
    password = models.CharField(max_length=100, blank=True)

    def clean(self):
        auth = None
        if self.username != '' and self.password != '':
            auth = (self.username, self.password)

        error = None
        try:
            r = requests.request(url=self.url, method=self.method, params=json.loads(self.params),
                                 data=json.loads(self.data), headers=json.loads(self.headers), auth=auth)
            if not r.ok:
                error = ValidationError(
                    _('%(url)s request error.'),
                    params={'url': self.url}
                )
            else:
                r.json()
        except RequestException:
            error = ValidationError(
                _('Error establishing connection with URL %(url)s.'),
                params={'url': self.url}
            )
        except ValueError:
            error = ValidationError(
                _('Response is not JSON serializable.')
            )
        finally:
            if error is not None:
                raise error

    def save(self, *args, **kwargs):
        self.conexion_type = 'API'
        self.url = 'http://{host}:{port}{endpoint}'.format(**vars(self))
        self.full_clean()
        super(ConexionAPI, self).save(*args, **kwargs)


class GAView(models.Model):
    VIEW_TYPE_CHOICES = (
        ('DB', 'DB'),
        ('API', 'API')
    )
    id = models.AutoField(primary_key=True)
    nombre = models.CharField(max_length=100, unique=True)
    conexion = models.ForeignKey(Conexion, on_delete=models.DO_NOTHING)
    columns = models.TextField(validators=[validate_json])
    view_type = models.CharField(max_length=10, choices=VIEW_TYPE_CHOICES)

    def save(self, *args, **kwargs):
        self.view_type = self.conexion.conexion_type
        if self.view_type == 'DB':
            conexion = ConexionDB.objects.get(id=self.conexion.id)
            sqla_string = conexion.sqla_string
            engine = create_engine(sqla_string)
            inspector = inspect(engine)
            columns = inspector.get_columns(conexion.table)
            self.columns = json.dumps([columns['name'] for columns in columns])
        elif self.view_type == 'API':
            f = lambda d: repr(d)[0] in '{[' and sum(
                [[str(k)] + ['%s.' % k + q for q in f(v)] for k, v in (enumerate, dict.items)['{' < repr(d)](d)],
                []) or []
            conexion = ConexionAPI.objects.get(id=self.conexion.id)
            auth = None
            if conexion.username != '' and conexion.password != '':
                auth = (conexion.username, conexion.password)
            r = requests.request(url=conexion.url, method=conexion.method, params=json.loads(conexion.params),
                                 data=json.loads(conexion.data), headers=json.loads(conexion.headers), auth=auth)
            columns = f(r.json())
            self.columns = json.dumps(columns)
        super(GAView, self).save(*args, **kwargs)
