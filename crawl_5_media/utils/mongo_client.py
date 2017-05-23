# -*- coding: utf-8 -*-

import pymongo
from crawl_5_media.localsettings import *

def get_mongo_client(client=None):
    # MONGODB_HOST = MONGODB_HOST
    # MONGODB_PORT = MONGODB_PORT
    USER_NAME = MONGO_USER_NAME
    PWD = MONGO_PWD

    if not client:
        client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
        client.admin.authenticate(USER_NAME, PWD)
    return client


def get_test_mongo_client(client=None):
    MONGO_CONN_STR = TEST_MONGO_CONN_STR
    if not client:
        client = pymongo.MongoClient(MONGO_CONN_STR)
    return client