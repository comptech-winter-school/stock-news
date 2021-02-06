import json
import ssl
import time
import urllib
from datetime import timedelta

import pandas as pd
import pymongo as pym
import yfinance as yf
from envparse import env
from gdeltdoc import Filters, GdeltDoc

from consts import ADDITIONAL_DIR


def get_shift_percentage(numerator: pd.Series,
                         denominator: pd.Series) -> pd.Series:
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


def get_data_yfinance(quotation: str,
                      start_date: str,
                      end_date: str,
                      interval='1d') -> pd.DataFrame:
    """
    Взять данные с yahoo finance
    params:
        quotation: название котировки, данные для которой хотим получить
        start_date, end_date - интервал, формат "год-месяц-день"
        interval - периодичность, формат "(номер)(первая буква слова (d, m, y))"
    returns:
        DataFrame формата "Тикет, Время, 6 видов цен"
    """

    df_res = yf.download(tickers=quotation,
                         start=start_date,
                         end=end_date,
                         interval=interval)
    df_res.loc[:, 'Ticker'] = quotation
    df_res = df_res.groupby(pd.Grouper(level="Date",
                                       freq=interval.upper())).mean()
    # Приводим время к одному виду для слияния
    df_res.index = pd.to_datetime(df_res.index).tz_localize('Etc/UCT')
    return df_res


def get_data_gdelt(quotation: str,
                   keywords: list,
                   start_date: str,
                   end_date: str,
                   interval="1d",
                   num_records=250,
                   repeats=3) -> pd.DataFrame:
    """
    Взять данные с gdelt
    params:
        quotation - имя ценной бумаги
        keywords - из графа знаний по ключевому слову
        start_date, end_date - интервал, формат "год-месяц-день"
        (опционально) interval - периодичность, формат "(номер)(первая буква слова (d, m, y))"
        (не реализована) (опционально) num_records - сколько максимум записей взять за промежуток
        (не реализовано) (опционально) repeats - сколько раз должно повториться ключевое слово в статье
    returns:
        DataFrame формата "Datetime (индекс), Ticker,
        [Average_Tone, Article_Count, Volume_Intensity]_[std, mean, sum, min, max]
    """

    # Колонки в полученных DataFrames
    col_names = ['Average_Tone', 'Article_Count', 'Volume_Intensity']

    # Что будем искать для данных ключевых слов
    # Тон статей, их количество и их кол-во в отношении ко всем остальным на gdelt
    match_list = ["timelinetone", "timelinevolraw", "timelinevol"]
    match_dict = dict(zip(match_list, col_names))

    df_res = None
    for keyword in keywords:
        try:
            gd_filter = Filters(start_date=start_date,
                                end_date=end_date,
                                keyword=keyword)

            for feature_name in match_list:
                gd = GdeltDoc()
                timeline_data = gd.timeline_search(feature_name, gd_filter)
                time.sleep(5)
                timeline_data = timeline_data.fillna(0)
                timeline_data = timeline_data.groupby(
                    pd.Grouper(key="datetime", freq=interval.upper()))

                if feature_name in ['timelinetone']:
                    timeline_data = timeline_data.mean()
                else:
                    timeline_data = timeline_data.sum()

                if df_res is None:
                    # Выровняем индексы, чтобы при копировании не выдавалось NaN
                    df_res = pd.DataFrame(index=timeline_data.index)

                col_name = match_dict[feature_name]
                df_res[f"{keyword}_{feature_name}_{col_name}"] = timeline_data[
                    col_name.replace('_', ' ')].values
        except Exception as e:
            print(f'invalid keyword: {keyword}')

    return df_res


def set_statistic_columns(df_dub: pd.DataFrame) -> pd.DataFrame:
    """После использования этой функции желательно удалить df_dub"""

    # Колонки в полученных DataFrames
    col_names = ['Average_Tone', 'Article_Count', 'Volume_Intensity']

    df_res = pd.DataFrame(index=df_dub.index)

    # Нужно создать колонки со средним, средним отклонением, минимумом и максимумом для каждой фичи
    # Сначала сформируем список датафреймов, которые нам нужно достать для каждой колонки
    for pattern in col_names:
        pattern_list = list()
        for col in df_dub.columns:
            if pattern in col:
                pattern_list.append(col)

        # Теперь для pattern у нас есть список
        df_res[f'{pattern}_min'] = df_dub[pattern_list].min(axis=1,
                                                            skipna=True)
        df_res[f'{pattern}_max'] = df_dub[pattern_list].max(axis=1,
                                                            skipna=True)
        df_res[f'{pattern}_mean'] = df_dub[pattern_list].mean(axis=1,
                                                              skipna=True)
        df_res[f'{pattern}_std'] = df_dub[pattern_list].std(axis=1,
                                                            skipna=True)
        df_res[f'{pattern}_sum'] = df_dub[pattern_list].sum(axis=1,
                                                            skipna=True)

    return df_res


def set_column_ticker(df: pd.DataFrame, quotation: str) -> None:
    # Добавим название ценной бумаги в таблицу
    df.loc[:, 'Ticker'] = quotation


def get_nan_chain(nan_series: pd.Series) -> list:
    """Берет цепочки NaN из фрейма"""
    res = []
    day = timedelta(days=1)
    along = list(nan_series[nan_series].index)

    prev_day = along[0]
    chain = [prev_day]
    for cur_day in along[1:]:
        if cur_day - prev_day == day:
            chain.append(cur_day)
        else:
            res.append(chain)
            chain = [cur_day]

        prev_day = cur_day

    return res


def set_cumulative_effect(df: pd.DataFrame, chains: list) -> None:
    """Меняет переданный датафрейм, не убирая NaN"""
    day = timedelta(days=1)

    for chain in chains:
        buffer = None
        for date in chain:
            if buffer is None:
                buffer = df.loc[date]
            else:
                buffer += df.loc[date]

        cumulative_day = chain[-1] + day
        # Берем все дни из цепочки, в строке будет среднее арифметическое этих дней
        df.loc[cumulative_day] = (df.loc[cumulative_day] +
                                  buffer) / (len(chain) + 1)


def get_dataframe_v2(**kwargs) -> (pd.DataFrame, pd.DataFrame):
    """
    Получить полный датафрейм с кумулятивностью и дополнительными фичами
    Пример использования: d = get_dataframe(quotation='NVDA',
                                            keywords=['nvidia', 'geforce', 'geforce rtx', 'geForce now',
                                            'nvidia rtx', 'nvidia shield', 'nvidia dgx'],
                                            start_date="2020-01-01",
                                            end_date="2020-12-31")
    params:
        quotation - имя ценной бумаги
        keywords - из графа знаний по ключевому слову
        start_date, end_date - интервал, формат "год-месяц-день"
        (опционально) interval - периодичность, формат "(номер)(первая буква слова (d, m, y))"
        (не реализована) (опционально) num_records - сколько максимум записей взять за промежуток
        (не реализовано) (опционально) repeats - сколько раз должно повториться ключевое слово в статье
    returns:
        DataFrame формата "Datetime (индекс), Ticker,
        [Average_Tone, Article_Count, Volume_Intensity]_[std, mean, sum, min, max], - из новостей
        Open, High, Low, Close, Adj Close, Volume - из финансов
    """

    gdelt_data = get_data_gdelt(**kwargs)
    yfinance_data = get_data_yfinance(
        quotation=kwargs['quotation'],
        start_date=kwargs['start_date'],
        end_date=kwargs['end_date'],
        interval="1d" if not kwargs.get('interval') else kwargs['interval'])

    row_is_nan = yfinance_data['Close'].isna()
    yfinance_data.dropna(inplace=True)
    set_cumulative_effect(gdelt_data, get_nan_chain(row_is_nan))
    gdelt_data.drop(row_is_nan[row_is_nan].index, inplace=True)
    gdelt_data = set_statistic_columns(gdelt_data)

    set_column_ticker(gdelt_data, quotation=kwargs['quotation'])
    set_column_ticker(yfinance_data, quotation=kwargs['quotation'])
    yfinance_data['Price Change'] = get_shift_digit(
        yfinance_data['Close'].dropna())
    yfinance_data['Percentage Change'] = get_shift_percentage(
        yfinance_data['Price Change'], yfinance_data['Close'])
    gdelt_data = gdelt_data.iloc[1:-1].reset_index()
    yfinance_data = yfinance_data.reset_index()

    return gdelt_data, yfinance_data


def get_keywords_for_one(company_name, limit=5):
    env.read_envfile()
    api_key = env("API_KEY")

    query = company_name + ' Company'
    service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
    params = {
        'query': query,
        'limit': limit,
        'indent': True,
        'key': api_key,
    }
    url = service_url + '?' + urllib.parse.urlencode(params)
    response = json.loads(urllib.request.urlopen(url).read())
    keywords = []
    for element in response['itemListElement']:
        # print(element['result']['name'] + ' (' + str(element['resultScore']) + ')')
        keywords.append(element['result']['name'])
    return list(filter(lambda x: len(x) >= 5, keywords))


def get_keywords_full(path):
    with open(path) as json_file:
        data = json.load(json_file)
        keywords = {i: get_keywords_for_one(data[i]['ticket'], limit=10) for i in range(len(data))}
    return keywords


def parse_dataframes_to_mongo(data_json, ssl_path=None):
    env.read_envfile()
    url = env("URL")
    client = pym.MongoClient(url,
                             ssl_ca_certs=ssl_path,
                             ssl_cert_reqs=ssl.CERT_REQUIRED)
    db = client['stock-news-backend']
    db_gdelt = db['GDELT']
    db_yfinance = db['YFINANCE']
    db_keywords = db['KEYWORDS']

    db_gdelt.drop()
    db_yfinance.drop()

    #     db_gdelt.create_index([("datetime", pym.ASCENDING),("Ticker", pym.ASCENDING)], unique=True)
    #     db_yfinance.create_index([("Date", pym.ASCENDING),("Ticker", pym.ASCENDING)], unique=True)

    with open(data_json) as json_file:
        data = json.load(json_file)
        for i in range(len(data)):
            try:
                print('TICKER:', data[i]['ticket'])
                keywords = db_keywords.find_one({"Ticker": data[i]['ticket']}, {'_id': 0})['Keywords']
                print('PARSED KEYWORDS', keywords)
                df_gdelt, df_yfinance = get_dataframe_v2(quotation=data[i]['ticket'],
                                                         keywords=keywords,
                                                         start_date="2017-01-01",
                                                         end_date="2020-12-31")

                db_gdelt.insert_many(df_gdelt.to_dict('records'))
                db_yfinance.insert_many(df_yfinance.to_dict('records'))
            except Exception as e:
                print('ERROR!', e)


def main():
    ssl_path = str(ADDITIONAL_DIR / 'YandexInternalRootCA.crt')
    json_path = str(ADDITIONAL_DIR / 'stocks.json')
    # keywords = get_keywords_full(json_path)
    parse_dataframes_to_mongo(json_path, ssl_path=ssl_path)


if __name__ == '__main__':
    main()
