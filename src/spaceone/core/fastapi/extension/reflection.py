import logging
from fastapi import APIRouter, Request

_LOGGER = logging.getLogger(__name__)

router = APIRouter()


def _add_mounted_apis(app, path=None):
    apis = []
    for route in app.routes:
        apis.append({'path': f'{path}{route.path}', 'name': route.name, 'method': route.methods})
    return apis


@router.get('/api/reflection')
async def api_reflection(request: Request):
    response = {'apis': []}

    for route in request.app.routes:
        if not hasattr(route, 'methods'):
            response['apis'].append({'path': route.path, 'name': route.name, 'method': []})
            response['apis'].extend(_add_mounted_apis(route.app, route.path))
        else:
            response['apis'].append({'path': route.path, 'name': route.name, 'method': route.methods})
    return response


