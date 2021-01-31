class ClickHouse:
    @staticmethod
    def read_settings_async():
        from envparse import env
        env.read_envfile()

        config = dict()
        config["url"] = env("CH_URL")
        config["user"] = env("CH_USER")
        config["password"] = env("CH_PASS")

        return config

    @staticmethod
    async def init_async(config):
        from aiohttp import ClientSession
        from aiochclient import ChClient as Client

        session = ClientSession()
        client = Client(session, **config)
        assert await client.is_alive()

        connect = {
            "client": client,
            "session": session
        }
        return connect

    @staticmethod
    async def close_async(connect):
        if not connect["session"].closed:
            await connect["session"].close()


class MySQL:
    @staticmethod
    def read_settings_async():
        from envparse import env
        env.read_envfile()

        config = dict()
        config["host"] = env("MYSQL_HOST")
        config["port"] = int(env("MYSQL_PORT"))

        mysql_user = env("MYSQL_USER", "")
        if mysql_user:
            config["user"] = mysql_user

        mysql_password = env("MYSQL_PASS", "")
        if mysql_password:
            config["password"] = mysql_password

        config["db"] = env("MYSQL_DB")

        return config

    @staticmethod
    async def init_async(config):
        import aiomysql

        config["autocommit"] = True
        connection = await aiomysql.connect(**config)
        cursor = await connection.cursor()

        connect = {
            "cursor": cursor,
            "connection": connection
        }
        return connect

    @staticmethod
    async def close_async(connect):
        await connect["cursor"].close()
        connect["connection"].close()

    @staticmethod
    def read_settings():
        return MySQL.read_settings_async()

    @staticmethod
    def init(config):
        import MySQLdb

        config["autocommit"] = True
        connection = MySQLdb.connect(**config)
        cursor = connection.cursor()

        connect = {
            "connection": connection,
            "cursor": cursor
        }
        return connect

    @staticmethod
    def close(connect):
        connect["cursor"].close()


class MongoDB:
    @staticmethod
    def read_settings_async():
        from envparse import env
        env.read_envfile()

        config = dict()
        config["connection_string"] = env("MONGODB_CONNECTION_STRING")
        config["db"] = env("MONGODB_DB")

        return config

    @staticmethod
    async def init_async(config):
        import motor.motor_asyncio as aiomotor

        conn = aiomotor.AsyncIOMotorClient(config["connection_string"])
        db = conn[config["db"]]

        connect = {
            "client": db
        }
        return connect

    @staticmethod
    async def close_async(connect):
        connect['client'].client.close()
