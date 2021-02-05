FROM python:3.8

# Setting Moscow time-zone
ENV TZ=Europe/Moscow
# hadolint ignore=DL3008
RUN ln -snf "/usr/share/zoneinfo/${TZ}" /etc/localtime && \
    echo "${TZ}" > /etc/timezone && \
    apt-get install --no-install-recommends -y tzdata default-libmysqlclient-dev && \
    dpkg-reconfigure --frontend noninteractive tzdata

WORKDIR /opt/project

# Устанавливаем пакеты Python
COPY ./backend/requirements.dev.txt .
RUN pip install --no-cache-dir -r requirements.dev.txt

# Копируем папку с приложением
COPY app app
COPY server.py server.py

# Создаем папку с логами
RUN mkdir logs

# Копируем файлы конфигураций
RUN mkdir config
COPY config/app.yaml config/app.yaml
RUN touch ./.env

CMD ["python", "server.py"]
