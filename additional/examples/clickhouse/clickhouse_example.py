from clickhouse_driver import connect
from envparse import env


def query_preprocess(query):
    return ' '.join(map(str.strip, query.split()))


def load_conf():
    config = {}
    config['clickhouse'] = dict(
        host=env("CH_URL"),
        user=env("CH_USER"),
        password=env("CH_PASS"),
        database=env("CH_DB"),
        port=9440
    )
    return config


def fire_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def main():
    env.read_envfile()
    config = load_conf()
    ch_conn = connect(
        secure=True,
        **config['clickhouse']
    )

    cursor = ch_conn.cursor()
    q = f"""
        SELECT now();
    """
    q = query_preprocess(q)
    print(q)

    result = fire_query(cursor, q)
    print(result)


if __name__ == '__main__':
    main()
