from datetime import datetime, timedelta
from typing import Optional

import motor.motor_asyncio as aiomotor

from app.db import DBS
from app.models import GetLevelMongo, GetLevelResponse
from app.utils import _get_days


def _get_date_range(end_date: str, days_back: int):
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    start_date = end_date - timedelta(days=days_back)
    return {
        (start_date + timedelta(days=x)).strftime("%Y-%m-%d"): x+1
        for x in range(0, (end_date - start_date).days)
    }


async def _get_level(level_id: int, model_id: str) -> Optional[GetLevelResponse]:
    db: aiomotor.AsyncIOMotorDatabase = DBS['mongo']
    model_coll: aiomotor.AsyncIOMotorCollection = db.get_collection(model_id.upper())

    level_mongo = await model_coll.find_one({'level_id': level_id})
    if level_mongo:
        d_b, _ = _get_days(model_id)

        level_mongo = GetLevelMongo.parse_obj(level_mongo)
        dates = _get_date_range(level_mongo.date, d_b)

        new_news = []
        for news in level_mongo.news:
            new_news += [(dates[news[0]], news[1])]

        lvl_json = level_mongo.dict()
        lvl_json['news'] = new_news
        return GetLevelResponse(
            dates=list(dates.keys()),
            **lvl_json,
        )

    return None
