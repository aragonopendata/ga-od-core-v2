from drf_yasg.generators import OpenAPISchemaGenerator
from drf_yasg import openapi


class HttpsSchemaGenerator(OpenAPISchemaGenerator):
    def get_paths_object(self, paths):
        paths.move_to_end('/GA_OD_Core/views', last=False)
        return openapi.Paths(paths=paths)
