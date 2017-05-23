# -*- coding: utf-8 -*-

import redis
import hashlib
from crawl_5_media.localsettings import *

def get_redis_client(client=None, db=0):
    real_redis_host = REDIS_HOST
    port = REDIS_PORT
    pwd = REDIS_PWD

    if not client:
        client = redis.StrictRedis(host=real_redis_host, port=port, password=pwd, db=db)
    return client


def check_hash_md5_url_at_redis(url, redis_client, redis_set_key, is_success=False):    
    hash_val = hashlib.md5(url).hexdigest()
    if not is_success:
        has_flag = redis_client.sismember(redis_set_key, hash_val)
        if has_flag:
            return False
        else:
            return True
    else:
        redis_client.sadd(redis_set_key, hash_val)
