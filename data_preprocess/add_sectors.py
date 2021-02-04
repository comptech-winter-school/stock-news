import pandas as pd


def add_sectors(dataset,path_to_json):
    ticker_sector = pd.read_json(path_to_json).drop(['name', 'date'], axis=1)

    return pd.merge(dataset,ticker_sector, left_on="Ticker",right_on="ticket")
