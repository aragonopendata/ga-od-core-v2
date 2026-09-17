"""
WSGI config_data for gaodcore project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/3.1/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'gaodcore_project.settings')


def client_ip_middleware(app):
    """Set REMOTE_ADDR to the original client IP when behind a reverse proxy.

    django-easy-audit reads the IP from the WSGI environ in the request_started
    signal, before any Django middleware runs, and stores it in a NOT NULL
    column. Rewriting the environ here keeps the real client IP in the audit log
    behind a proxy while still working when Django is reached directly.
    """
    def wrapped(environ, start_response):
        forwarded_for = environ.get('HTTP_X_FORWARDED_FOR')
        if forwarded_for:
            environ['REMOTE_ADDR'] = forwarded_for.split(',')[0].strip()
        elif not environ.get('REMOTE_ADDR'):
            environ['REMOTE_ADDR'] = '0.0.0.0'  # noqa: S104
        return app(environ, start_response)

    return wrapped


application = client_ip_middleware(get_wsgi_application())
