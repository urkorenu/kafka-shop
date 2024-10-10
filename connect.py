import certifi
from pymongo.mongo_client import MongoClient
from config import db_url


def connect_to_mongo():
    uri = db_url

    client = MongoClient(uri, tlsCAFile=certifi.where())

    try:
        client.admin.command("ping")
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    db = client["kafka"]
    return db
