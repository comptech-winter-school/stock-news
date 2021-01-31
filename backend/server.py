import uvicorn
from fastapi import FastAPI, APIRouter, Request

from app.db import init_databases, shutdown_databases
from app.settings import load_config, CONFIG
from app.settings.consts import VERSION, SERVICE_NAME, MSG_SERVICE_DESCRIPTION
from app.settings.logging import init_logging

router = APIRouter()


@router.on_event("startup")
async def startup():
    await init_databases(CONFIG)


@router.on_event("shutdown")
async def shutdown():
    await shutdown_databases()


@router.get("/self_check/")
async def self_check(r: Request):
    return {"status": "Ok"}


def init_app():
    load_config()
    init_logging()

    app = FastAPI(
        title=SERVICE_NAME, description=MSG_SERVICE_DESCRIPTION, version=VERSION,
    )

    app.include_router(router, prefix=f"/{SERVICE_NAME}")

    return app


def run():
    app = init_app()
    uvicorn.run(app, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    run()
