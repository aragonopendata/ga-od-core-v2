"""User account views for the private Manager/Health web interface.

Authentication itself (credential checks, session creation, password
verification) is entirely delegated to Django's own `django.contrib.auth`
views; this module only supplies project-owned templates, the staff-only
profile page, and the login destination.
"""

from django.contrib.auth import views as auth_views
from django.urls import reverse_lazy
from django.utils.decorators import method_decorator
from django.views.generic import TemplateView

from gaodcore_manager.auth import staff_required


class AccountLoginView(auth_views.LoginView):
    template_name = "gaodcore_manager/account/login.html"

    def get_default_redirect_url(self):
        return str(reverse_lazy("manager_web:resource-list"))


class AccountLogoutView(auth_views.LogoutView):
    # Logout only accepts POST (with CSRF protection), so a state-changing
    # GET request is rejected with 405 rather than silently logging out.
    http_method_names = ["post", "options"]
    template_name = "gaodcore_manager/account/logged_out.html"


@method_decorator(staff_required, name="dispatch")
class ProfileView(TemplateView):
    """Read-only account summary for the current staff user.

    Only ever reads `request.user`; there is no user-id path or query
    parameter, so it cannot be made to display another user's data.
    """

    template_name = "gaodcore_manager/account/profile.html"


@method_decorator(staff_required, name="dispatch")
class AccountPasswordChangeView(auth_views.PasswordChangeView):
    template_name = "gaodcore_manager/account/password_change.html"
    success_url = reverse_lazy("account:password_change_done")


@method_decorator(staff_required, name="dispatch")
class AccountPasswordChangeDoneView(auth_views.PasswordChangeDoneView):
    template_name = "gaodcore_manager/account/password_change_done.html"
