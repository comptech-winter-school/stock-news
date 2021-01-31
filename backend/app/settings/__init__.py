import yaml

from app.settings.consts import SERVICE_NAME, CONFIG_DIR

CONFIG = dict()


def load_config():
    with open(CONFIG_DIR / "app.yaml") as f:
        CONFIG["app"] = yaml.safe_load(f)

    # Delete what you don't need
    # CONFIG["clickhouse"] = ClickHouse.read_settings_async()
    # CONFIG["mysql"] = MySQL.read_settings_async()
