import logging
from fastapi import APIRouter, Request
from fastapi.openapi.utils import get_openapi

_LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.get('/api/reflection')
async def api_reflection(request: Request):
    return {
        'apis': [
            {'path': route.path, 'name': route.name, 'method': route.methods}
            for route in request.app.routes
        ]
    }


