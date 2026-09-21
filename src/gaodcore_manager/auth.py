"""Shared staff-access check for the private Manager/Health web interface.

Unlike `django.contrib.admin.views.decorators.staff_member_required`, this
sends anonymous and non-staff users to the interface's own login page
(`account:login`) instead of the Django Admin login, leaving Django Admin's
own authentication untouched.
"""

from django.contrib.auth.decorators import user_passes_test
from django.urls import reverse_lazy


def _is_active_staff(user):
    return user.is_authenticated and user.is_active and user.is_staff


staff_required = user_passes_test(_is_active_staff, login_url=reverse_lazy("account:login"))
