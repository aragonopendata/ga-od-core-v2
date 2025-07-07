"""
Custom preprocessing hooks for drf-spectacular schema generation.
Replaces the functionality of the old HttpsSchemaGenerator.
"""


def custom_preprocessing_hook(endpoints):
    """
    Custom preprocessing hook that:
    1. Filters out paths ending with '{format}' (Django format_suffix_patterns duplicates)
    2. Filters out admin endpoints (paths starting with '/GA_OD_Core_admin/')
    3. Only includes public API endpoints (paths starting with '/GA_OD_Core/')
    4. Reorders paths to prioritize '/GA_OD_Core/views'

    Args:
        endpoints: List of endpoint tuples (path, method, callback)

    Returns:
        List of filtered and reordered endpoint tuples
    """

    # Filter out format-suffixed paths and admin endpoints
    # Django's format_suffix_patterns creates duplicate endpoints with {format} suffix
    filtered_endpoints = []
    for endpoint in endpoints:
        path = endpoint[0]

        # Skip admin endpoints
        if path.startswith('/GA_OD_Core_admin/'):
            continue

        # Only include GA_OD_Core endpoints
        if not path.startswith('/GA_OD_Core/'):
            continue

        # Skip format-suffixed endpoints created by Django's format_suffix_patterns
        if '{format}' in path:
            continue

        filtered_endpoints.append(endpoint)

    # Reorder endpoints - prioritize '/GA_OD_Core/views' at the beginning
    prioritized_endpoints = [
        endpoint for endpoint in filtered_endpoints
        if endpoint[0].startswith('/GA_OD_Core/views')
    ] + [
        endpoint for endpoint in filtered_endpoints
        if not endpoint[0].startswith('/GA_OD_Core/views')
    ]

    return prioritized_endpoints


def admin_preprocessing_hook(endpoints):
    """
    Custom preprocessing hook for admin API that:
    1. Filters out paths ending with '{format}' (Django format_suffix_patterns duplicates)
    2. Only includes admin endpoints (paths starting with '/GA_OD_Core_admin/')

    Args:
        endpoints: List of endpoint tuples (path, method, callback)

    Returns:
        List of filtered endpoint tuples
    """
    # Filter out format-suffixed paths and only include admin endpoints
    filtered_endpoints = []
    for endpoint in endpoints:
        path = endpoint[0]

        # Only include admin endpoints
        if not path.startswith('/GA_OD_Core_admin/'):
            continue

        # Skip format-suffixed endpoints created by Django's format_suffix_patterns
        if '{format}' in path:
            continue

        filtered_endpoints.append(endpoint)

    return filtered_endpoints


def parameter_order_hook(result, generator, request, public):
    """
    Postprocessing hook to maintain parameter order as defined in the old drf-yasg version.
    The correct order should be:
    1. resource_id, 2. view_id, 3. filters, 4. offset, 5. limit,
    6. fields/columns, 7. like, 8. sort, 9. formato, 10. nameRes/name,
    11. _page, 12. _pageSize
    """
    # Define the desired parameter order (excluding 'format' which should be handled via Accept header)
    parameter_order = [
        'resource_id', 'view_id', 'filters', 'offset', 'limit',
        'fields', 'columns', 'like', 'sort', 'formato',
        'nameRes', 'name', '_page', '_pageSize'
    ]

    # Reorder parameters in each endpoint and remove redundant format parameters
    if 'paths' in result:
        for path, path_data in result['paths'].items():
            for method, method_data in path_data.items():
                if 'parameters' in method_data:
                    parameters = method_data['parameters']

                    # Filter out the automatic 'format' parameter added by drf-spectacular
                    # We use content negotiation via Accept headers instead
                    filtered_parameters = [param for param in parameters if param['name'] != 'format']

                    # Create a mapping of parameter names to parameter objects
                    param_map = {param['name']: param for param in filtered_parameters}

                    # Reorder parameters according to the desired order
                    ordered_params = []

                    # First, add parameters in the desired order
                    for param_name in parameter_order:
                        if param_name in param_map:
                            ordered_params.append(param_map[param_name])

                    # Then add any remaining parameters not in the order list
                    for param in filtered_parameters:
                        if param['name'] not in parameter_order:
                            ordered_params.append(param)

                    # Update the parameters list
                    method_data['parameters'] = ordered_params

    return result
