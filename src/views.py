"""Module that contains Mixins to improve Django views."""

from rest_framework.views import APIView
from serializers import DictSerializer


class APIViewMixin(APIView):
    """Mixin that implements get_serializer due that external resources are provided by SQLAlchemy."""
    def get_serializer(self, *args, **kwargs):
        """Function that provides a custom serializer to serialize external resources. Django ModelViewSet implements
        get_serializer but all external resources use SQLAlchemy ORM instead of Django ORM."""
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser
