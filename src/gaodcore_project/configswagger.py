from drf_yasg.generators import OpenAPISchemaGenerator
from collections import OrderedDict
from drf_yasg import openapi

class HttpsSchemaGenerator(OpenAPISchemaGenerator):
    def get_schema(self, request=None, public=False):
        schema = super().get_schema(request, public)
        schema.schemes = ["https", "http"]
        return schema

    def get_paths_object(self, paths):
        paths.move_to_end('/GA_OD_Core/views', last=False)
        return openapi.Paths(paths=paths)