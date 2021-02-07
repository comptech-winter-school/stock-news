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