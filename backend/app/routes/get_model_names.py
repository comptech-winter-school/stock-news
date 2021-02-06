import motor.motor_asyncio as aiomotor

from app.db import DBS
from app.models import GetModelNamesResponse
from app.utils import _get_days

NAMES = None


async def _get_model_names() -> GetModelNamesResponse:
    global NAMES
    # fast cache
    if not NAMES:
        db: aiomotor.AsyncIOMotorDatabase = DBS['mongo']
        _filter = lambda x: x.startswith("MODEL_")
        model_list = await db.list_collection_names()

        res = []
        for model_id in model_list:
            if not _filter(model_id):
                continue

            d_b, d_f = _get_days(model_id)
            res += [{
                'model_id': model_id,
                'name': f"Информация за {d_b} дней, предсказываем на {d_f} дней"
            }]

        NAMES = GetModelNamesResponse(data=res)

    return NAMES
