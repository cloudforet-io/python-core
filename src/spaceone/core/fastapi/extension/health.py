import logging
from fastapi import APIRouter

_LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.get('/check')
async def check():
    return {'status': 'SERVING'}
