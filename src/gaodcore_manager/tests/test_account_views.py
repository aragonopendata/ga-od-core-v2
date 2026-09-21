from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse

PRIVATE_PREFIX = "/admin/GA_OD_Core_admin/"


class AccountTestCase(TestCase):

    def setUp(self):
        self.staff_user = User.objects.create_user("staff", password="a-test-password", is_staff=True)
        self.regular_user = User.objects.create_user("regular", password="a-test-password", is_staff=False)


class TestAccountRouteNamesResolveToExactPaths(AccountTestCase):

    def test_account_routes_reverse_to_the_exact_required_paths(self):
        expected = {
            "account:login": f"{PRIVATE_PREFIX}login/",
            "account:logout": f"{PRIVATE_PREFIX}logout/",
            "account:profile": f"{PRIVATE_PREFIX}profile/",
            "account:password_change": f"{PRIVATE_PREFIX}password-change/",
            "account:password_change_done": f"{PRIVATE_PREFIX}password-change/done/",
        }
        for name, expected_path in expected.items():
            with self.subTest(name=name):
                self.assertEqual(reverse(name), expected_path)


class TestLogin(AccountTestCase):

    def test_login_page_is_available_anonymously_and_uses_the_custom_template(self):
        response = self.client.get(reverse("account:login"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "gaodcore_manager/account/login.html")
        self.assertNotIn("admin/css/base.css", response.content.decode())

    def test_successful_login_with_no_next_redirects_to_the_manager_resource_list(self):
        response = self.client.post(
            reverse("account:login"),
            {
                "username": "staff",
                "password": "a-test-password"
            },
        )
        self.assertRedirects(response, reverse("manager_web:resource-list"))

    def test_successful_login_honors_a_safe_next_value(self):
        target = reverse("manager_web:connector-list")
        response = self.client.post(
            f"{reverse('account:login')}?next={target}",
            {
                "username": "staff",
                "password": "a-test-password"
            },
        )
        self.assertRedirects(response, target)

    def test_successful_login_ignores_an_unsafe_next_value(self):
        response = self.client.post(
            f"{reverse('account:login')}?next=https://evil.example/",
            {
                "username": "staff",
                "password": "a-test-password"
            },
        )
        self.assertRedirects(response, reverse("manager_web:resource-list"))


class TestLogout(AccountTestCase):

    def test_logout_is_rendered_as_a_post_form_with_csrf_protection(self):
        self.client.force_login(self.staff_user)
        response = self.client.get(reverse("manager_web:resource-list"))
        content = response.content.decode()
        self.assertIn(f'action="{reverse("account:logout")}"', content)
        self.assertIn('method="post"', content)
        self.assertIn("csrfmiddlewaretoken", content)

    def test_get_logout_does_not_log_out_and_returns_405(self):
        self.client.force_login(self.staff_user)
        response = self.client.get(reverse("account:logout"))
        self.assertEqual(response.status_code, 405)
        # The session is untouched: a subsequent staff page load still succeeds.
        response = self.client.get(reverse("manager_web:resource-list"))
        self.assertEqual(response.status_code, 200)

    def test_post_logout_clears_the_session_and_reaches_the_logged_out_page(self):
        self.client.force_login(self.staff_user)
        response = self.client.post(reverse("account:logout"), follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "gaodcore_manager/account/logged_out.html")
        response = self.client.get(reverse("manager_web:resource-list"))
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith(f"{PRIVATE_PREFIX}login/"))


class TestProfile(AccountTestCase):

    def test_anonymous_users_are_redirected_to_the_custom_login(self):
        response = self.client.get(reverse("account:profile"))
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith(f"{PRIVATE_PREFIX}login/"))

    def test_non_staff_users_cannot_view_the_profile(self):
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse("account:profile"))
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith(f"{PRIVATE_PREFIX}login/"))

    def test_staff_users_can_view_their_own_profile(self):
        self.staff_user.first_name = "Jane"
        self.staff_user.last_name = "Doe"
        self.staff_user.email = "jane@example.com"
        self.staff_user.save()
        self.client.force_login(self.staff_user)
        response = self.client.get(reverse("account:profile"))
        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        self.assertIn("staff", content)
        self.assertIn("Jane", content)
        self.assertIn("Doe", content)
        self.assertIn("jane@example.com", content)

    def test_profile_only_ever_shows_the_logged_in_user_regardless_of_query_params(self):
        other_staff = User.objects.create_user("other-staff",
                                               password="a-test-password",
                                               is_staff=True,
                                               email="other@example.com")
        self.client.force_login(self.staff_user)
        response = self.client.get(reverse("account:profile"), {"user": other_staff.pk, "id": other_staff.pk})
        content = response.content.decode()
        self.assertIn("staff", content)
        self.assertNotIn("other-staff", content)
        self.assertNotIn("other@example.com", content)

    def test_profile_does_not_expose_sensitive_account_fields(self):
        self.client.force_login(self.staff_user)
        response = self.client.get(reverse("account:profile"))
        content = response.content.decode()
        self.assertNotIn(self.staff_user.password, content)
        self.assertNotIn("is_superuser", content)
        self.assertNotIn("groups", content)


class TestPasswordChange(AccountTestCase):

    def test_password_change_works_for_the_current_staff_user(self):
        self.client.force_login(self.staff_user)
        response = self.client.post(
            reverse("account:password_change"),
            {
                "old_password": "a-test-password",
                "new_password1": "a-new-test-password",
                "new_password2": "a-new-test-password",
            },
        )
        self.assertRedirects(response, reverse("account:password_change_done"))
        self.staff_user.refresh_from_db()
        self.assertTrue(self.staff_user.check_password("a-new-test-password"))

    def test_anonymous_users_are_redirected_to_the_custom_login(self):
        response = self.client.get(reverse("account:password_change"))
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith(f"{PRIVATE_PREFIX}login/"))


class TestHeaderUserMenu(AccountTestCase):

    def test_shared_header_shows_the_current_username_and_account_urls(self):
        self.client.force_login(self.staff_user)
        for url in (reverse("manager_web:resource-list"), reverse("gaodcore_health:connector_list")):
            with self.subTest(url=url):
                response = self.client.get(url)
                content = response.content.decode()
                self.assertIn("staff", content)
                self.assertIn(reverse("account:profile"), content)
                self.assertIn(reverse("account:password_change"), content)
                self.assertIn(reverse("account:logout"), content)
