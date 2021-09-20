from collections import OrderedDict

from drf_yasg.generators import OpenAPISchemaGenerator
from drf_yasg import openapi
from drf_yasg.openapi import PathItem


class HttpsSchemaGenerator(OpenAPISchemaGenerator):
    def get_paths_object(self, paths: OrderedDict[str, PathItem]):
        for key in list(paths.keys()):
            if key.endswith('{format}'):
                del paths[key]

        paths.move_to_end('/GA_OD_Core/views', last=False)
        return openapi.Paths(paths=paths)
