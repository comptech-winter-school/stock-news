Установите сертификат:
```
mkdir -p ~/.clickhouse-client /usr/local/share/ca-certificates/Yandex && \
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt && \
wget "https://storage.yandexcloud.net/mdb/clickhouse-client.conf.example" -O ~/.clickhouse-client/config.xml
```

Установите зависимости:
`pip install requests`

Пример кода:
```
import requests

def request():
    url = 'https://{host}:8443/?database={db}&query={query}'.format(
        host='ЗАПРОС У ВАЛЕНТИНА',
        db='ЗАПРОС У ВАЛЕНТИНА',
        query='SELECT now()')
    auth = {
        'X-ClickHouse-User': 'ЗАПРОС У ВАЛЕНТИНА',
        'X-ClickHouse-Key': 'ЗАПРОС У ВАЛЕНТИНА',
    }

    res = requests.get(
        url,
        headers=auth,
        verify='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt')
    res.raise_for_status()
    return res.text

print(request())
```
