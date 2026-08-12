from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from semantic_search.qdrant_store import ensure_collections, get_client
from web.routers import admin, episodes, podcasts, search, topics


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.qdrant = get_client()
    await ensure_collections(app.state.qdrant)
    yield
    await app.state.qdrant.close()


app = FastAPI(title="PodIntel", lifespan=lifespan)

static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

app.include_router(search.router)
app.include_router(podcasts.router)
app.include_router(episodes.router)
app.include_router(topics.router)
app.include_router(admin.router)
