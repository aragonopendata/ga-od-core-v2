from rest_framework.views import APIView
from serializers import DictSerializer

class APIViewMixin(APIView):
    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser
