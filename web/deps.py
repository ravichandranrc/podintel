from fastapi import Header, HTTPException, Request
from qdrant_client import AsyncQdrantClient

from common.config import get_settings


async def require_admin(x_admin_key: str = Header(default="")) -> None:
    settings = get_settings()
    if not x_admin_key or x_admin_key != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="invalid or missing X-Admin-Key")


async def get_qdrant(request: Request) -> AsyncQdrantClient:
    return request.app.state.qdrant
