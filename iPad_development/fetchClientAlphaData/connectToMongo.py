from pymongo.errors import ServerSelectionTimeoutError
from configparser import ConfigParser
from pymongo import MongoClient


def connectToMongo():
    """
    Connects to MongoDB using config.ini
    Returns:
        MongoClient
    """

    # ---------- Read Config ----------
    configReader = ConfigParser()
    configReader.read("config.ini")

    host = configReader.get("DBParams", "host")
    port = configReader.getint("DBParams", "port")
    username = configReader.get("DBParams", "username")
    password = configReader.get("DBParams", "password")
    auth_db = configReader.get("DBParams", "auth_db", fallback="admin")

    try:
        # ---------- Create Client ----------
        client = MongoClient(
            host=host,
            port=port,
            username=username,
            password=password,
            authSource=auth_db,
            serverSelectionTimeoutMS=5000,   # Fail fast (5 sec)
            connectTimeoutMS=5000,           # Socket connect timeout
            socketTimeoutMS=5000             # Read/write timeout
        )

        # ---------- Force Connection Check ----------
        client.admin.command("ping")

        print("MongoDB connected successfully")

    except ServerSelectionTimeoutError as e:
        raise Exception(f"MongoDB server not reachable: {e}")

    except Exception as e:
        raise Exception(f"MongoDB connection failed: {e}")

    return client


# connectToMongo()