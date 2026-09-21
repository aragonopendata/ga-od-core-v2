from django.urls import path

from gaodcore_manager.account_views import (
    AccountLoginView,
    AccountLogoutView,
    AccountPasswordChangeDoneView,
    AccountPasswordChangeView,
    ProfileView,
)

app_name = "account"

urlpatterns = [
    path("login/", AccountLoginView.as_view(), name="login"),
    path("logout/", AccountLogoutView.as_view(), name="logout"),
    path("profile/", ProfileView.as_view(), name="profile"),
    path("password-change/", AccountPasswordChangeView.as_view(), name="password_change"),
    path(
        "password-change/done/",
        AccountPasswordChangeDoneView.as_view(),
        name="password_change_done",
    ),
]
