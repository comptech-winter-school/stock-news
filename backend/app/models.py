"""
Here you can write your pydantic models
"""
from typing import List, Dict, Tuple

from pydantic import BaseModel


class GetModelNamesResponse(BaseModel):
    data: List[Dict[str, str]]


class GetLevelRequest(BaseModel):
    model_id: str
    level_id: int


class GetLevelMongo(BaseModel):
    """
    выход:
        график yfinance за промежуток времени,
        тональность новостей за промежуток времени,
        кол-во новостей за день
        промежуток времени,
        хайп новости 5 шт,
    """
    level_id: int
    prices: List[float]
    tones: List[float]
    volumes: List[int]
    news: List[Tuple[str, str]]
    model_predict: int
    target: int
    date: str
    days_back: int


class GetLevelResponse(BaseModel):
    dates: List[str]
    prices: List[float]
    tones: List[float]
    volumes: List[int]
    news: List[Tuple[str, str]]
    model_predict: int
    target: int


def main():
    r = GetLevelMongo(
        level_id=0,
        prices=[1., 2., 3.],
        tones=[3., 2., -3.],
        volumes=[100, 23201, 123123],
        news=[
            ('2020-02-13', 'Умер тигренок'),
            ('2020-02-15', 'Умер жирафик'),
            ('2020-02-19', 'Умер бегемотик'),
        ],
        model_predict=1,
        date='2020-02-20',
        days_back=30
    )
    import json
    print(json.dumps(r.dict()))


if __name__ == '__main__':
    main()