from fastapi import FastAPI

from app.routes.api import router as api_router
from app.settings import get_settings

settings = get_settings()

app = FastAPI(title=settings.app_name)
app.include_router(api_router)
