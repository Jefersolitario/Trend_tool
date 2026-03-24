import os

import certifi
from pymongo import MongoClient


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def mongo_client():
    return MongoClient(
        f"mongodb://app_power_dashboard:jQl6TZMfYe61Brbs@dev1-shard-00-00.uvhb7.mongodb.net:27017,dev1-shard-00-01.uvhb7.mongodb.net:27017,dev1-shard-00-02.uvhb7.mongodb.net:27017/test?authSource=admin&replicaSet=atlas-k6fhv2-shard-0&ssl=true",
        tz_aware=True,
        w="majority",
        readpreference="primary",
        journal=True,
        wTimeoutMS=60000,
        connect=False,
        tlsCAFile=certifi.where(),
        maxPoolSize=200,
    )


client = mongo_client()