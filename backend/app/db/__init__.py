from app.db.wrappers import ClickHouse

DBS = {}


async def init_databases(config):
    """
    Usage example
    DBS["clickhouse"] = await ClickHouse.init_async(config["clickhouse"])
    DBS["mysql"] = await MySQL.init_async(config["mysql"])
    """
    pass


async def shutdown_databases():
    """
    await ClickHouse.close_async(DBS["clickhouse"])
    await MySQL.close_async(DBS["mysql"])
    """
    pass
