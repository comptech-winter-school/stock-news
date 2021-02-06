from typing import Dict

import pandas as pd
import streamlit as st
import altair as alt
import requests

from app.SessionState import SessionState, get
from app.consts import BACKEND_URL
from app.models import Level


@st.cache(ttl=100)
def _method(suburl, data=None) -> Dict:
    if data:
        return requests.post(
            f'{BACKEND_URL}{suburl}',
            json=data
        ).json()
    else:
        return requests.get(f'{BACKEND_URL}{suburl}',).json()


def model_names() -> Dict:
    data = _method('/get_model_names/')
    data = data['data']
    d = {}
    for item in data:
        d[item['model_id']] = item['name']
    return d


# @st.cache(ttl=100)
def get_level(level_id: int, model_id: str) -> Level:
    level_json = _method('/get_level/', data={
        'level_id': level_id,
        'model_id': model_id
    })
    # st.write(level_json)
    if level_json.get('status'):
        return None
    return Level.parse_obj(level_json)


def plot_level(level: Level):
    df = pd.DataFrame()
    # df['dates'] = level.dates[]
    df['prices'] = level.prices
    df['tones'] = level.tones
    df['volumes'] = level.volumes
    df = df.reset_index()
    df['index'] += 1

    chart = alt.Chart(df).mark_area(
        color="lightblue",
        # interpolate='step-after',
        line=True
    ).encode(
        x=alt.X('index:N',
                # scale=alt.Scale(domain=0),
                title='Номер дня'
                ),
        y=alt.Y('prices:Q',
                # scale=alt.Scale(),
                title='Цена закрытия'
                ),
        tooltip=[alt.Tooltip('tones', title='Тональность за день'),
                 alt.Tooltip('volumes', title='Количество статей')]
    ).properties(
        title='Stock News Game'
    ).interactive()
    chart.height = 400
    chart.width = 600
    return chart


def show_news(level):
    df = pd.DataFrame(level.news)
    df.columns = ['Индекс дня', "Новость"]
    df_ = df.to_html(index=False)
    st.markdown(df_, unsafe_allow_html=True)


def main():
    SessionState(level=0, model_score=0, user_score=0)

    get_state = get(level=0, model_score=0, user_score=0)
    # st.title(f'Вы {get_state.user_score}:{get_state.model_score} Модель')
    st.markdown('## Stock News!')

    model_names_d = model_names()
    model_id = st.selectbox(
        'Выбирайте модель для сражения:',
        list(model_names_d.keys()),
        format_func=lambda x: model_names_d[x]
    )

    level: Level = get_level(get_state.level, model_id)

    def score_round(level: Level, user_answer):
        if level.target == user_answer:
            get_state.user_score += 1


    if not level:
        st.write('На сегодня Ваши уровни закончились :)')
        _str = f'Вы ' + str(get_state.user_score) + ':' + str(get_state.model_score) + ' Модель'
        st.title(_str)
    else:
        chart = plot_level(level)
        col1, col2, col3 = st.beta_columns(3)

        _str = f'Вы ' + str(get_state.user_score) + ':' + str(get_state.model_score) + ' Модель'
        st.title(_str)

        def score_round(answer):
            nonlocal level
            if level.target == answer and level.model_predict == level.target:
                get_state.user_score += 1
                get_state.model_score += 1
                get_state.level += 1
            elif level.target != answer and level.model_predict == level.target:
                get_state.model_score += 1
                get_state.level += 1
            elif level.target == answer and level.model_predict != level.target:
                get_state.user_score += 1
                get_state.level += 1
            else:
                get_state.level += 1

        if col3.button('Растет!'):
            score_round(1)
        if col2.button('Пропуск'):
            get_state.level += 1
        if col1.button('Падает!'):
            score_round(0)

        st.altair_chart(chart, use_container_width=True)
        show_news(level)


    # st.markdown(f'# Человек {get_state.user_score}:{get_state.model_score} Модель')


if __name__ == '__main__':
    main()
