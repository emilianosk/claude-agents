import logging

from fastapi import FastAPI

from app.routes.api import router as api_router
from app.settings import get_settings

settings = get_settings()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
)

app = FastAPI(title=settings.app_name)
app.include_router(api_router)
