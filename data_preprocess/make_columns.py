import pandas as pd


def get_shift_percentage(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """
    На сколько процентов изменилась переменная numerator
    относительно текущего denominator
    """
    return numerator / denominator * 100


def get_shift_digit(s: pd.Series) -> pd.Series:
    """
    Возвращает колонку с изменениями значений
    Для вызова этой функции необходимо очистить колонки Close от NaN
    """
    assert not s.isna().any(), "Необходимо очистить колонку от NaN"

    return (s.shift(-1) - s).shift(1)
