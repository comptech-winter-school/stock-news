from typing import List, Tuple

from pydantic import BaseModel


class Level(BaseModel):
    dates: List[str]
    prices: List[float]
    tones: List[float]
    volumes: List[float]
    news: List[Tuple[str, str]]
    model_predict: int
    target: int
    Ticker: str
    company_name: str
    wiki_info: str = None