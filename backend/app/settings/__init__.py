from app.db import MongoDB

CONFIG = dict()


def load_config():
    CONFIG["mongo"] = MongoDB.read_settings_async()
