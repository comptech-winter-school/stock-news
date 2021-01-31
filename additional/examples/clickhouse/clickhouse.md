# COMPTECH: clickhouse

Установите сертификат:
```
mkdir -p ~/.clickhouse-client /usr/local/share/ca-certificates/Yandex && \
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt && \
wget "https://storage.yandexcloud.net/mdb/clickhouse-client.conf.example" -O ~/.clickhouse-client/config.xml
```

Установите зависимости:
`pip install clickhouse_driver envparse`

Переименуйте файл `sample.env` в `.env`, а после заполните поля

Запускайте)