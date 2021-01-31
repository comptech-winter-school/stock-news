from fastapi.testclient import TestClient

from app.db import init_databases, shutdown_databases
from app.settings import CONFIG
from server import init_app

app = init_app(use_sentry=False)


@app.on_event("startup")
async def startup():
    await init_databases(CONFIG)


@app.on_event("shutdown")
async def shutdown():
    await shutdown_databases()


def test_self_check():
    with TestClient(app) as client:
        response = client.get("/stock_news/self_check/")

        assert response.status_code == 200
        print(response.json())
        assert response.json() == {"status": "Ok"}
