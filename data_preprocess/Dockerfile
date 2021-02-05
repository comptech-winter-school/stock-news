FROM python:3.8

# Setting Moscow time-zone
ENV TZ=Europe/Moscow
RUN ln -snf "/usr/share/zoneinfo/${TZ}" /etc/localtime && \
    echo "${TZ}" > /etc/timezone && \
    apt-get install --no-install-recommends -y tzdata default-libmysqlclient-dev && \
    dpkg-reconfigure --frontend noninteractive tzdata

# Копируем все файлы из текущей директории в /app контейнера
COPY ./ /app
# Устанавливаем все зависимости
RUN pip install -r /app/requirements.txt --no-cache-dir

CMD ["python", "/app/data_preprocess/upload_data2mongo.py"]
