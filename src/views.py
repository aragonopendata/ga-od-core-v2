"""Module that contains Mixins to improve Django views."""

from rest_framework.views import APIView
from serializers import DictSerializer


class APIViewMixin(APIView):
    """Mixin that implements get_serializer due that external resources are provided by SQLAlchemy."""

    def get_serializer(self, *args, **kwargs):
        """Function that provides a custom serializer to serialize external resources. Django ModelViewSet implements
        get_serializer but all external resources use SQLAlchemy ORM instead of Django ORM."""
        # This check prevents this issue https://github.com/axnsan12/drf-yasg/issues/333
        if getattr(self, 'swagger_fake_view', False):
            return None  # Short-circuit schema generation
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser
